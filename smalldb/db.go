package smalldb

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// DB is a simple in-memory key/value store backed by a write-ahead log (WAL).
//
// Reads are concurrent; writes are serialized to ensure WAL records are appended
// (and fsync'd) before the corresponding state mutation is applied.
type DB struct {
	dir     string
	version uint64

	stateMu sync.RWMutex
	state   map[string][]byte

	// Serializes writers so we can do (validate -> WAL fsync -> apply) without
	// interleaving WAL records.
	writeMu sync.Mutex

	wal    *wal
	closed bool
}

// Open opens (or creates) a database at opts.Dir.
//
// Open replays the write-ahead log to rebuild the in-memory state,
// then opens the WAL for appending new records.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("options.Dir is empty")
	}
	if err := ensureDir(opts.Dir, opts.CreateIfMissing); err != nil {
		return nil, err
	}

	// Version switch recovery: finalizes any pending switch and prunes stale
	// generations so the directory has a single active (checkpoint, log) pair.
	version, err := recoverAndCleanupVersionSwitch(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := writeVersionFileIfMissing(opts.Dir, version); err != nil {
		return nil, err
	}

	db := &DB{
		dir:     opts.Dir,
		version: version,
	}

	// Recovery:
	// - Load checkpoint.<version> into memory (if it exists).
	// - Replay logfile.<version> on top.
	//
	// This preserves the core invariant: checkpoint + subsequent log
	// deterministically reconstruct the latest committed state.
	state, err := loadCheckpoint(opts.Dir, version)
	if err != nil {
		return nil, err
	}
	db.state = state

	if err := replayWAL(logPath(opts.Dir, version), func(r walRecord) error {
		switch r.op {
		case opSet:
			// Copy value so the backing slice isn't reused.
			v := make([]byte, len(r.value))
			copy(v, r.value)
			db.state[r.key] = v
			return nil
		case opDelete:
			delete(db.state, r.key)
			return nil
		default:
			return fmt.Errorf("unknown wal op: %d", r.op)
		}
	}); err != nil {
		return nil, err
	}

	w, err := openWAL(logPath(opts.Dir, version))
	if err != nil {
		return nil, err
	}
	db.wal = w
	return db, nil
}

// Get returns a copy of the value for key.
//
// The returned bool reports whether the key exists.
func (db *DB) Get(key string) ([]byte, bool, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, false, err
	}

	db.stateMu.RLock()
	defer db.stateMu.RUnlock()

	v, ok := db.state[key]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

// Set stores value at key.
//
// Set appends a WAL record and fsyncs it before applying the update to the
// in-memory state. The value is copied, so callers can safely reuse the input
// slice after Set returns.
func (db *DB) Set(key string, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if key == "" {
		return errors.New("key is empty")
	}

	if err := db.wal.appendSet(key, value); err != nil {
		return err
	}

	db.stateMu.Lock()
	defer db.stateMu.Unlock()

	v := make([]byte, len(value))
	copy(v, value)
	db.state[key] = v
	return nil
}

// Delete removes key if present.
//
// Delete appends a WAL record and fsyncs it before applying the deletion to the
// in-memory state.
func (db *DB) Delete(key string) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if key == "" {
		return errors.New("key is empty")
	}

	if err := db.wal.appendDelete(key); err != nil {
		return err
	}

	db.stateMu.Lock()
	defer db.stateMu.Unlock()
	delete(db.state, key)
	return nil
}

// Checkpoint persists the current state in a compact form and truncates the WAL.
func (db *DB) Checkpoint() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}

	// Checkpoint protocol:
	// - Block writers (writeMu) but keep allowing readers.
	// - Take a stable snapshot of the in-memory map.
	// - Write checkpoint.(N+1) via temp file + fsync + rename.
	// - Create logfile.(N+1).
	// - Publish newVersion (commit point), then finalize the switch and rotate WAL.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Take a consistent snapshot while allowing concurrent readers.
	db.stateMu.RLock()
	snapshot := cloneStateLocked(db.state)
	db.stateMu.RUnlock()

	next := db.version + 1
	if err := writeCheckpoint(db.dir, next, snapshot); err != nil {
		return err
	}

	// After writing the checkpoint (N+1), perform the version switch steps:
	// 1) Create empty logfile.(N+1)
	if err := createEmptyLogFile(db.dir, next); err != nil {
		_ = os.Remove(checkpointPath(db.dir, next))
		return fmt.Errorf("create next log file: %w", err)
	}

	// 2) Write newVersion containing (N+1) and fsync it (commit point)
	if err := writeNewVersionFile(db.dir, next); err != nil {
		// Not committed: safe to remove the unreferenced new generation files.
		_ = os.Remove(checkpointPath(db.dir, next))
		_ = os.Remove(logPath(db.dir, next))
		_ = os.Remove(newVersionTempPath(db.dir))
		return fmt.Errorf("write new version file: %w", err)
	}

	// 3) Delete old checkpoint.N, logfile.N
	if err := cleanupOldGenerationFiles(db.dir, db.version); err != nil {
		return fmt.Errorf("cleanup old files: %w", err)
	}

	// 4) Rename newVersion -> version and fsync directory
	if err := finalizeVersionSwitch(db.dir); err != nil {
		return fmt.Errorf("finalize version switch: %w", err)
	}

	// 5) Update in-memory pointers:
	if err := db.wal.close(); err != nil {
		return fmt.Errorf("close old wal: %w", err)
	}
	newWal, err := openWAL(logPath(db.dir, next))
	if err != nil {
		return fmt.Errorf("open new wal: %w", err)
	}
	db.wal = newWal
	db.version = next

	return nil
}

// Close closes the database and releases file descriptors.
//
// Close is safe to call multiple times.
func (db *DB) Close() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if db.wal != nil {
		return db.wal.close()
	}
	return nil
}

// ensureOpen returns an error if the DB has been closed.
func (db *DB) ensureOpen() error {
	if db.closed {
		return errors.New("db is closed")
	}
	return nil
}

// ensureDir validates that dir exists (or creates it when createIfMissing is
// true) and that it is a directory.
func ensureDir(dir string, createIfMissing bool) error {
	st, err := os.Stat(dir)
	if err == nil {
		if !st.IsDir() {
			return fmt.Errorf("path is not a directory: %s", dir)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat directory: %w", err)
	}
	if !createIfMissing {
		return fmt.Errorf("directory does not exist: %s", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	return nil
}

func cloneStateLocked(src map[string][]byte) map[string][]byte {
	dst := make(map[string][]byte, len(src))
	for k, v := range src {
		cp := make([]byte, len(v))
		copy(cp, v)
		dst[k] = cp
	}
	return dst
}
