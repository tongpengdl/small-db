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

	version, err := resolveActiveVersion(opts.Dir)
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

	// Milestone 4 (checkpoint restore):
	// 1) Load checkpoint.<version> into memory (if it exists).
	// 2) Replay logfile.<version> on top.
	//
	// This preserves the paper’s recovery invariant: checkpoint + subsequent log
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

	// Checkpoint serialize:
	// - Block writers (writeMu) but keep allowing readers.
	// - Take a stable snapshot of the in-memory map.
	// - Serialize snapshot to checkpoint.(N+1) via temp file + fsync + rename.
	//
	// Note: the “version switch + log rotation” is Milestone 5. For now,
	// checkpointing can be implemented as “write a snapshot file” without
	// switching the active generation.
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
	// TODO(Milestone 5): After checkpointing, rotate to logfile.(N+1) and
	// atomically switch `version`/`newVersion` to point at N+1.
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
