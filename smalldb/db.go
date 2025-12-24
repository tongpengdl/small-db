package smalldb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DB is a simple in-memory key/value store backed by a write-ahead log (WAL).
//
// Reads are concurrent; writes are serialized to ensure WAL records are appended
// (and fsync'd) before the corresponding state mutation is applied.
type DB struct {
	dir     string
	version uint64
	policy  checkpointPolicy

	stateMu sync.RWMutex
	state   map[string][]byte

	// Serializes writers so we can do (validate -> WAL fsync -> apply) without
	// interleaving WAL records.
	writeMu sync.Mutex

	wal    *wal
	closed bool

	lastCheckpointUnixNano atomic.Int64
	updatesSinceCheckpoint atomic.Uint64
	walBytes               atomic.Int64

	commitCh   chan writeRequest
	commitDone chan struct{}

	bgStop     chan struct{}
	bgDone     chan struct{}
	bgWake     chan struct{}
	bgStopOnce sync.Once
}

type writeRequest struct {
	op    byte
	key   string
	value []byte
	resp  chan error
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
		dir:        opts.Dir,
		version:    version,
		policy:     policyFromOptions(opts),
		commitCh:   make(chan writeRequest, 100),
		commitDone: make(chan struct{}),
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
		return applyRecord(db.state, r)
	}); err != nil {
		return nil, err
	}

	w, err := openWAL(logPath(opts.Dir, version))
	if err != nil {
		return nil, err
	}
	db.wal = w

	if st, err := db.wal.f.Stat(); err == nil {
		db.walBytes.Store(st.Size())
	}
	db.lastCheckpointUnixNano.Store(time.Now().UnixNano())

	go db.groupCommitLoop()
	db.startBackgroundCheckpointing()

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
// Set sends a write request to the group commit loop and waits for it to be
// persisted and applied.
func (db *DB) Set(key string, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if key == "" {
		return errors.New("key is empty")
	}

	req := writeRequest{
		op:    opSet,
		key:   key,
		value: value,
		resp:  make(chan error, 1),
	}
	select {
	case db.commitCh <- req:
	case <-db.commitDone:
		return errors.New("db is closed")
	}

	return <-req.resp
}

// Delete removes key if present.
//
// Delete sends a write request to the group commit loop and waits for it to be
// persisted and applied.
func (db *DB) Delete(key string) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if key == "" {
		return errors.New("key is empty")
	}

	req := writeRequest{
		op:   opDelete,
		key:  key,
		resp: make(chan error, 1),
	}
	select {
	case db.commitCh <- req:
	case <-db.commitDone:
		return errors.New("db is closed")
	}

	return <-req.resp
}

func (db *DB) groupCommitLoop() {
	defer close(db.commitDone)

	// Batch buffer
	batch := make([]writeRequest, 0, 128)

	for {
		// 1. Collect a batch
		// Block until at least one request arrives or we are stopped.
		req, ok := <-db.commitCh
		if !ok {
			return
		}
		batch = append(batch, req)

		// Opportunistically drain the channel to fill the batch.
		// We use a small limit to ensure we don't starve latency too much,
		// though fsync is the dominator anyway.
	drainLoop:
		for len(batch) < 128 {
			select {
			case req, ok := <-db.commitCh:
				if !ok {
					// Channel closed, stop draining and commit what we have.
					break drainLoop
				}
				batch = append(batch, req)
			default:
				// No more immediate requests, proceed to commit.
				break drainLoop
			}
		}

		// 2. Commit the batch
		db.commitBatch(batch)

		// 3. Reset for next iteration
		// Clear slice but keep capacity
		for i := range batch {
			batch[i] = writeRequest{} // avoid memory leaks
		}
		batch = batch[:0]
	}
}

func (db *DB) commitBatch(batch []writeRequest) {
	// Acquire write lock to serialize against Checkpoint.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// If WAL is closed (e.g. Checkpoint switched it, or Close called), ensure we are safe.
	// But Checkpoint holds writeMu while switching, so we are safe.
	// Only weird case: DB closed? Close() acquires writeMu too.
	if db.closed || db.wal == nil {
		for _, req := range batch {
			req.resp <- errors.New("db is closed")
		}
		return
	}

	// 1. Write all to WAL (buffered)
	var batchBytes int64
	for i, req := range batch {
		n, err := db.wal.writeRecord(req.op, req.key, req.value)
		if err != nil {
			// If write fails, we can't commit. Fail the whole batch?
			// Yes, integrity is compromised if we continue.
			failBatch(batch[i:], err)
			return // Partial write? DB might need restart.
		}
		batchBytes += int64(n)
	}

	// 2. Fsync
	if err := db.wal.Sync(); err != nil {
		failBatch(batch, err)
		return
	}

	// 3. Apply to state and Ack
	// We hold writeMu, so no other writer/checkpointer can interfere.
	// We need stateMu for map access (readers).
	db.stateMu.Lock()
	for _, req := range batch {
		err := applyRecord(db.state, walRecord{op: req.op, key: req.key, value: req.value})
		// applying strictly shouldn't fail if logic is correct
		req.resp <- err
	}
	db.stateMu.Unlock()

	db.noteCommittedUpdate(batchBytes)
}

func failBatch(batch []writeRequest, err error) {
	for _, req := range batch {
		req.resp <- err
	}
}

// Checkpoint persists the current state in a compact form and truncates the WAL.
func (db *DB) Checkpoint() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	return db.checkpointLocked()
}

func (db *DB) checkpointLocked() error {
	// Checkpoint protocol:
	// - Block writers (writeMu) but keep allowing readers.
	// - Take a stable snapshot of the in-memory map.
	// - Write checkpoint.(N+1) via temp file + fsync + rename.
	// - Create logfile.(N+1).
	// - Publish newVersion (commit point), then finalize the switch and rotate WAL.

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
	db.updatesSinceCheckpoint.Store(0)
	db.walBytes.Store(0)
	db.lastCheckpointUnixNano.Store(time.Now().UnixNano())

	return nil
}

// Close closes the database and releases file descriptors.
//
// Close is safe to call multiple times.
func (db *DB) Close() error {
	// 1. Stop background checkpointing
	db.stopBackgroundCheckpointing()

	// 2. Stop group commit loop
	// We close the channel to signal the loop to exit.
	// We need to ensure we don't close it twice.
	db.writeMu.Lock()
	if db.closed {
		db.writeMu.Unlock()
		return nil
	}
	db.closed = true
	// Close commit channel to drain and stop the loop
	close(db.commitCh)
	db.writeMu.Unlock()

	// Wait for loop to finish
	<-db.commitDone

	// 3. Close WAL
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if db.wal != nil {
		return db.wal.close()
	}
	return nil
}

// ensureOpen returns an error if the DB has been closed.
func (db *DB) ensureOpen() error {
	// This check is slightly racy but Set/Delete catch the closed channel.
	// The lock in Close ensures we don't accept new work after closing.
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

func applyRecord(state map[string][]byte, r walRecord) error {
	switch r.op {
	case opSet:
		v := make([]byte, len(r.value))
		copy(v, r.value)
		state[r.key] = v
		return nil
	case opDelete:
		delete(state, r.key)
		return nil
	default:
		return fmt.Errorf("unknown wal op: %d", r.op)
	}
}

func (db *DB) startBackgroundCheckpointing() {
	if !db.policy.enabled {
		return
	}
	db.bgStop = make(chan struct{})
	db.bgDone = make(chan struct{})
	db.bgWake = make(chan struct{}, 1)
	go db.backgroundCheckpointLoop()
}

func (db *DB) stopBackgroundCheckpointing() {
	if db.bgStop == nil {
		return
	}
	db.bgStopOnce.Do(func() { close(db.bgStop) })
	<-db.bgDone
}

func (db *DB) backgroundCheckpointLoop() {
	defer close(db.bgDone)

	ticker := time.NewTicker(db.policy.checkEvery)
	defer ticker.Stop()

	lastAttempt := time.Time{}
	for {
		select {
		case <-db.bgStop:
			return
		case <-ticker.C:
		case <-db.bgWake:
		}

		if !db.shouldCheckpointNow(time.Now()) {
			continue
		}
		if !lastAttempt.IsZero() && time.Since(lastAttempt) < db.policy.minRetryBack {
			continue
		}
		lastAttempt = time.Now()

		// Best-effort: if checkpoint fails, we retry later.
		//
		// Avoid blocking shutdown by only checkpointing when we can acquire the
		// writer lock without waiting.
		if !db.writeMu.TryLock() {
			continue
		}
		_ = db.checkpointLocked()
		db.writeMu.Unlock()
	}
}

func (db *DB) shouldCheckpointNow(now time.Time) bool {
	last := time.Unix(0, db.lastCheckpointUnixNano.Load())
	if db.policy.interval > 0 && now.Sub(last) >= db.policy.interval {
		return true
	}
	if db.policy.maxUpdates > 0 && db.updatesSinceCheckpoint.Load() >= db.policy.maxUpdates {
		return true
	}
	if db.policy.maxLogBytes > 0 && db.walBytes.Load() >= db.policy.maxLogBytes {
		return true
	}
	return false
}

func (db *DB) noteCommittedUpdate(walBytes int64) {
	db.updatesSinceCheckpoint.Add(1)
	db.walBytes.Add(walBytes)

	if db.bgWake != nil && db.shouldCheckpointNow(time.Now()) {
		select {
		case db.bgWake <- struct{}{}:
		default:
		}
	}
}
