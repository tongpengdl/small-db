package smalldb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// Checkpoint format
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

const checkpointFormatVersion uint32 = 1

// loadCheckpoint loads `checkpoint.version` if it exists and returns the state.
//
// - Open checkpointPath(dir, version) and parse header.
// - Validate magic + format version.
// - Deserialize the DB state into a fresh map[string][]byte.
// - If the file doesn't exist, return an empty map and nil error.
func loadCheckpoint(dir string, version uint64) (map[string][]byte, error) {
	path := checkpointPath(dir, version)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[string][]byte), nil
		}
		return nil, fmt.Errorf("open checkpoint: %w", err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 256*1024)

	var fileMagic [len(checkpointMagic)]byte
	if _, err := io.ReadFull(r, fileMagic[:]); err != nil {
		return nil, fmt.Errorf("read checkpoint magic: %w", err)
	}
	if string(fileMagic[:]) != checkpointMagic {
		return nil, fmt.Errorf("invalid checkpoint magic: %q", string(fileMagic[:]))
	}

	fileFormatVersion, err := readU32(r)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint format version: %w", err)
	}
	if fileFormatVersion != checkpointFormatVersion {
		return nil, fmt.Errorf("unsupported checkpoint format version: %d", fileFormatVersion)
	}

	fileVersion, err := readU64(r)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint generation: %w", err)
	}
	if fileVersion != version {
		return nil, fmt.Errorf("checkpoint version mismatch: got %d, want %d", fileVersion, version)
	}

	nEntries, err := readU32(r)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint entry count: %w", err)
	}

	state := make(map[string][]byte, int(nEntries))
	for i := uint32(0); i < nEntries; i++ {
		keyLen, err := readU32(r)
		if err != nil {
			return nil, fmt.Errorf("read key length: %w", err)
		}
		valLen, err := readU32(r)
		if err != nil {
			return nil, fmt.Errorf("read value length: %w", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return nil, fmt.Errorf("read key bytes: %w", err)
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBytes); err != nil {
			return nil, fmt.Errorf("read value bytes: %w", err)
		}

		state[string(keyBytes)] = valBytes
	}
	return state, nil
}

// writeCheckpoint writes a full snapshot to `checkpoint.version` using a crash-safe protocol.
//
// - Write to a temp file first (e.g., checkpoint.version.tmp).
// - fsync the temp file.
// - rename(temp, final) (atomic on POSIX).
// - fsync the directory so the rename is durable.
func writeCheckpoint(dir string, version uint64, snapshot map[string][]byte) (retErr error) {
	tmp := checkpointTempPath(dir, version)
	final := checkpointPath(dir, version)

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			_ = os.Remove(tmp)
			f, err = os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		}
	}
	if err != nil {
		return fmt.Errorf("create checkpoint temp file: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	w := bufio.NewWriterSize(f, 256*1024)
	if _, err := w.WriteString(checkpointMagic); err != nil {
		return fmt.Errorf("write checkpoint magic: %w", err)
	}
	if err := writeU32(w, checkpointFormatVersion); err != nil {
		return fmt.Errorf("write checkpoint format version: %w", err)
	}
	if err := writeU64(w, version); err != nil {
		return fmt.Errorf("write checkpoint generation: %w", err)
	}
	if err := writeU32(w, uint32(len(snapshot))); err != nil {
		return fmt.Errorf("write checkpoint entry count: %w", err)
	}
	for k, v := range snapshot {
		if err := writeU32(w, uint32(len(k))); err != nil {
			return fmt.Errorf("write key length: %w", err)
		}
		if err := writeU32(w, uint32(len(v))); err != nil {
			return fmt.Errorf("write value length: %w", err)
		}
		if _, err := w.WriteString(k); err != nil {
			return fmt.Errorf("write key bytes: %w", err)
		}
		if _, err := w.Write(v); err != nil {
			return fmt.Errorf("write value bytes: %w", err)
		}
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush checkpoint: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}

	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync checkpoint dir: %w", err)
	}
	return nil
}

func checkpointTempPath(dir string, version uint64) string {
	return checkpointPath(dir, version) + ".tmp"
}

func readU32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func readU64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

func writeU32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeU64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}
