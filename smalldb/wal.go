package smalldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	walFileName = "log.wal"

	opSet    byte = 1
	opDelete byte = 2
)

// wal implements an append-only write-ahead log used for crash recovery.
//
// Record format (LittleEndian):
//   - uint32 payload length
//   - byte op
//   - uint32 key length
//   - uint32 value length
//   - key bytes
//   - value bytes
type wal struct {
	f *os.File
}

// openWAL opens the WAL file for append-only writes, creating it if missing.
func openWAL(dir string) (*wal, error) {
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &wal{f: f}, nil
}

// close closes the WAL file handle.
func (w *wal) close() error {
	if w == nil || w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// appendSet appends a durable "set" record to the WAL.
func (w *wal) appendSet(key string, value []byte) error {
	return w.appendRecord(opSet, key, value)
}

// appendDelete appends a durable "delete" record to the WAL.
func (w *wal) appendDelete(key string) error {
	return w.appendRecord(opDelete, key, nil)
}

// appendRecord serializes a single record, writes it to disk, and fsyncs.
func (w *wal) appendRecord(op byte, key string, value []byte) error {
	if w == nil || w.f == nil {
		return errors.New("wal is closed")
	}
	keyBytes := []byte(key)
	// 1 byte op + 4 bytes key length + 4 bytes value length + key + value
	payloadLen := uint32(1 + 4 + 4 + len(keyBytes) + len(value))

	var buf bytes.Buffer
	buf.Grow(int(4 + payloadLen))
	_ = binary.Write(&buf, binary.LittleEndian, payloadLen)
	_ = buf.WriteByte(op)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(keyBytes)))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(value)))
	_, _ = buf.Write(keyBytes)
	_, _ = buf.Write(value)

	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("write wal: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("fsync wal: %w", err)
	}
	return nil
}

type walRecord struct {
	op    byte
	key   string
	value []byte
}

// replayWAL streams WAL records in order and calls apply for each decoded record.
//
// If the file ends with a partial record (e.g., a crash during append), the
// trailing fragment is ignored.
func replayWAL(dir string, apply func(walRecord) error) error {
	path := filepath.Join(dir, walFileName)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	var lenBuf [4]byte
	// We must use an unbounded loop here because the WAL is a stream of
	// back-to-back records with no upfront count or index.
	//
	// Each iteration reads and applies exactly ONE record:
	//   1) read the length prefix
	//   2) read the payload
	//   3) decode and apply
	//
	// The loop terminates based on I/O conditions, not a counter:
	//   - io.EOF            → clean end of WAL
	//   - io.ErrUnexpectedEOF → partial record from a crash; safely ignore tail
	//
	// This streaming pattern is essential for crash safety: it allows us to
	// replay all fully-written records while tolerating a torn or incomplete
	// record at the end of the file.
	for {
		_, err := io.ReadFull(f, lenBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read wal length: %w", err)
		}

		payloadLen := binary.LittleEndian.Uint32(lenBuf[:])
		if payloadLen == 0 || payloadLen > 64<<20 {
			return fmt.Errorf("invalid wal payload length: %d", payloadLen)
		}

		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(f, payload)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read wal payload: %w", err)
		}

		rec, err := decodeWALRecord(payload)
		if err != nil {
			return err
		}
		if err := apply(rec); err != nil {
			return err
		}
	}
}

// decodeWALRecord validates and decodes a single WAL payload (without the length prefix).
func decodeWALRecord(payload []byte) (walRecord, error) {
	if len(payload) < 1+4+4 {
		return walRecord{}, fmt.Errorf("wal payload too short: %d", len(payload))
	}
	op := payload[0]
	keyLen := binary.LittleEndian.Uint32(payload[1:5])
	valLen := binary.LittleEndian.Uint32(payload[5:9])

	need := int(1 + 4 + 4 + keyLen + valLen)
	if need != len(payload) {
		return walRecord{}, fmt.Errorf("wal payload length mismatch: have=%d want=%d", len(payload), need)
	}

	keyStart := 9
	keyEnd := keyStart + int(keyLen)
	valEnd := keyEnd + int(valLen)
	key := string(payload[keyStart:keyEnd])
	value := make([]byte, int(valLen))
	copy(value, payload[keyEnd:valEnd])

	switch op {
	case opSet, opDelete:
	default:
		return walRecord{}, fmt.Errorf("unknown wal op: %d", op)
	}

	return walRecord{op: op, key: key, value: value}, nil
}
