package smalldb

import (
	"errors"
	"fmt"
	"os"
)

// Checkpoint format (Milestone 4)
//
// Goal:
// - Persist a full, consistent snapshot of the in-memory state to
//   `checkpoint.N` (where N is a generation/version number).
// - On startup, load `checkpoint.N` to seed memory, then replay `logfile.N`.
//
// Recommended structure:
// - A small fixed-size header with a magic string and a format version.
// - Then the serialized key/value data (gob is fine to start).
// - Optionally a checksum to detect corruption.
//
// Suggested header fields (example):
//   magic:    []byte("SMALLDB\000")
//   version:  uint32
//   checksum: uint32 (or uint64) of the serialized payload
//
// NOTE: Only the "where to put code" scaffolding is provided here. The actual
// serialization and I/O protocol is left for you to implement.

const checkpointMagic = "SMALLDB\x00"

// loadCheckpoint loads `checkpoint.version` if it exists and returns the state.
//
// Milestone 4 tasks (you will implement):
// - Open checkpointPath(dir, version) and parse header.
// - Validate magic + format version.
// - Deserialize the DB state into a fresh map[string][]byte.
// - If the file doesn't exist, return an empty map and nil error.
func loadCheckpoint(dir string, version uint64) (map[string][]byte, error) {
	path := checkpointPath(dir, version)
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[string][]byte), nil
		}
		return nil, fmt.Errorf("stat checkpoint: %w", err)
	}

	// TODO(Milestone 4): Implement deserialization.
	// - Read/validate header.
	// - Decode payload (gob/binary/etc).
	// - Return the reconstructed state map.
	return nil, fmt.Errorf("checkpoint restore not implemented: %s", path)
}

// writeCheckpoint writes a full snapshot to `checkpoint.version` using a crash-safe protocol.
//
// Milestone 4 tasks (you will implement):
// - Write to a temp file first (e.g., checkpoint.version.tmp).
// - fsync the temp file.
// - rename(temp, final) (atomic on POSIX).
// - fsync the directory so the rename is durable.
func writeCheckpoint(dir string, version uint64, snapshot map[string][]byte) error {
	_ = snapshot

	// TODO(Milestone 4): Implement serialization + safe write/rename protocol.
	// Suggested steps:
	//   tmp := checkpointTempPath(dir, version)
	//   final := checkpointPath(dir, version)
	//   - create tmp (O_EXCL)
	//   - write header + payload
	//   - f.Sync()
	//   - f.Close()
	//   - os.Rename(tmp, final)
	//   - syncDir(dir)
	return errors.New("checkpoint write not implemented")
}

func checkpointTempPath(dir string, version uint64) string {
	return checkpointPath(dir, version) + ".tmp"
}
