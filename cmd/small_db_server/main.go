package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pengtong/small-db/smalldb"
)

type apiServer struct {
	db       *smalldb.DB
	readOnly bool
}

type setRequest struct {
	Value    string `json:"value"`
	Encoding string `json:"encoding,omitempty"`
}

type kvResponse struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding"`
}

type okResponse struct {
	OK bool `json:"ok"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	var (
		addrFlag        = flag.String("addr", ":8080", "listen address")
		dirFlag         = flag.String("dir", "", "database directory")
		createFlag      = flag.Bool("create", false, "create directory if missing")
		disableBgFlag   = flag.Bool("disable-background-checkpoint", false, "disable background checkpointing")
		intervalFlag    = flag.Duration("checkpoint-interval", 0, "checkpoint interval (0 uses default)")
		updatesFlag     = flag.Uint64("checkpoint-updates", 0, "checkpoint after N updates (0 disables)")
		logBytesFlag    = flag.Int64("checkpoint-log-bytes", 0, "checkpoint after N WAL bytes (0 uses default)")
		shutdownTimeout = flag.Duration("shutdown-timeout", 5*time.Second, "graceful shutdown timeout")
		backupOfFlag    = flag.String("backup-of", "", "primary address; when set, run in backup (read-only) mode")
	)
	flag.Parse()

	if *dirFlag == "" {
		log.Fatal("missing required -dir")
	}

	opts := smalldb.Options{
		Dir:                         *dirFlag,
		CreateIfMissing:             *createFlag,
		DisableBackgroundCheckpoint: *disableBgFlag,
		CheckpointInterval:          *intervalFlag,
		CheckpointUpdates:           *updatesFlag,
		CheckpointLogBytes:          *logBytesFlag,
	}
	if *backupOfFlag != "" {
		opts.DisableBackgroundCheckpoint = true
	}
	db, err := smalldb.Open(opts)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("close db: %v", err)
		}
	}()

	api := &apiServer{db: db, readOnly: *backupOfFlag != ""}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", api.handleHealth)
	mux.HandleFunc("/v1/kv/", api.handleKV)
	mux.HandleFunc("/v1/checkpoint", api.handleCheckpoint)

	server := &http.Server{
		Addr:    *addrFlag,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *backupOfFlag != "" {
		log.Printf("backup mode: replicating from %s", *backupOfFlag)
		log.Printf("backup mode: background checkpointing disabled")
		conn, err := net.Dial("tcp", *backupOfFlag)
		if err != nil {
			log.Fatalf("dial primary %s: %v", *backupOfFlag, err)
		}
		go func() {
			if err := db.ReceiveReplication(conn); err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("replication stream closed")
				} else {
					log.Printf("replication error: %v", err)
				}
				stop()
			}
		}()
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	log.Printf("small-db server listening on %s", *addrFlag)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, okResponse{OK: true})
}

func (s *apiServer) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
	if key == r.URL.Path || key == "" {
		writeError(w, http.StatusBadRequest, errors.New("key is required"))
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handleSet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (s *apiServer) handleGet(w http.ResponseWriter, _ *http.Request, key string) {
	value, ok, err := s.db.Get(key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("key not found: %s", key))
		return
	}
	resp := kvResponse{
		Key:      key,
		Value:    base64.StdEncoding.EncodeToString(value),
		Encoding: "base64",
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *apiServer) handleSet(w http.ResponseWriter, r *http.Request, key string) {
	if s.readOnly {
		writeError(w, http.StatusForbidden, errors.New("writes are disabled on backup"))
		return
	}
	var req setRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	value, err := decodeValue(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if err := s.db.Set(key, value); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, okResponse{OK: true})
}

func (s *apiServer) handleDelete(w http.ResponseWriter, _ *http.Request, key string) {
	if s.readOnly {
		writeError(w, http.StatusForbidden, errors.New("writes are disabled on backup"))
		return
	}
	if err := s.db.Delete(key); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, okResponse{OK: true})
}

func (s *apiServer) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if s.readOnly {
		writeError(w, http.StatusForbidden, errors.New("writes are disabled on backup"))
		return
	}
	if err := s.db.Checkpoint(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, okResponse{OK: true})
}

func decodeJSON(r io.Reader, dst any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("unexpected data after JSON object")
	}
	return nil
}

func decodeValue(req setRequest) ([]byte, error) {
	enc := strings.TrimSpace(strings.ToLower(req.Encoding))
	switch enc {
	case "", "base64":
		value, err := base64.StdEncoding.DecodeString(req.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 value: %w", err)
		}
		return value, nil
	case "utf-8", "utf8", "plain", "text":
		return []byte(req.Value), nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", req.Encoding)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}

func writeMethodNotAllowed(w http.ResponseWriter, allow ...string) {
	w.Header().Set("Allow", strings.Join(allow, ", "))
	writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}
