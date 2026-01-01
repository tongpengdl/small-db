package smalldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	opSet    byte = 1
	opDelete byte = 2
)

// wal implements an append-only write-ahead log used for crash recovery.
//
// WAL record format (LittleEndian):
//
//	[ uint32 payload_len ]
//	[ payload bytes ... ]        // logical operation only
//	[ uint32 crc32(payload) ]
//
// Payload layout:
//
//	[ byte   op ]
//	[ uint32 key_len ]
//	[ uint32 value_len ]
//	[ key bytes ]
//	[ value bytes ]
//
// Notes:
//   - The CRC is computed over the payload bytes only.
//   - The CRC is NOT part of the payload.
//   - A torn write may leave an incomplete trailing record, which is
//     safely ignored during replay.
type wal struct {
	f *os.File
}

// openWAL opens the WAL file for append-only writes, creating it if missing.
// It prepare a WAL such that future appends are safe, durable, and correctly replayable
func openWAL(path string) (*wal, error) {
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, fmt.Errorf("stat wal: %w", statErr)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	if created {
		// Ensure the directory entry is durable too; fsync'ing the file contents
		// is not sufficient to guarantee the newly-created name is persisted.
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("fsync wal after create: %w", err)
		}
		if err := syncDir(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, err
		}
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

// Sync forces a write of all buffered data to the stable storage.
func (w *wal) Sync() error {
	if w == nil || w.f == nil {
		return errors.New("wal is closed")
	}
	return w.f.Sync()
}

// writeRecord serializes a single record and writes it to the OS buffer.
// It does NOT fsync. Callers must call Sync() to make data durable.
func (w *wal) writeRecord(op byte, key string, value []byte) (int, error) {
	if w == nil || w.f == nil {
		return 0, errors.New("wal is closed")
	}

	recordBytes := encodeWALRecord(op, key, value)
	if err := writeAll(w.f, recordBytes); err != nil {
		return 0, fmt.Errorf("write wal: %w", err)
	}
	return len(recordBytes), nil
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
func replayWAL(path string, apply func(walRecord) error) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
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
		recordStart, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek wal: %w", err)
		}

		_, err = io.ReadFull(f, lenBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// Partial trailing record.
				_ = f.Truncate(recordStart)
				_ = f.Sync()
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
				// Partial trailing record.
				_ = f.Truncate(recordStart)
				_ = f.Sync()
				return nil
			}
			return fmt.Errorf("read wal payload: %w", err)
		}

		var crcBuf [4]byte
		_, err = io.ReadFull(f, crcBuf[:])
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// Partial trailing record.
				_ = f.Truncate(recordStart)
				_ = f.Sync()
				return nil
			}
			return fmt.Errorf("read wal crc: %w", err)
		}

		wantCRC := binary.LittleEndian.Uint32(crcBuf[:])
		gotCRC := crc32.ChecksumIEEE(payload)
		if gotCRC != wantCRC {
			// Stop replay at the first corrupted record. This preserves the
			// "prefix property": all earlier records were fully written and verified,
			// so the state remains correct up to the last good record.
			//
			// Best-effort truncate drops the corrupted record and any trailing bytes
			// so subsequent opens don't repeatedly hit the same mismatch.
			_ = f.Truncate(recordStart)
			_ = f.Sync()
			return nil
		}

		rec, err := decodeWALPayload(payload)
		if err != nil {
			return err
		}
		if err := apply(rec); err != nil {
			return err
		}
	}
}

func encodeWALPayload(op byte, key string, value []byte) []byte {
	keyBytes := []byte(key)
	// 1 byte op + 4 bytes key length + 4 bytes value length + key + value
	payloadLen := 1 + 4 + 4 + len(keyBytes) + len(value)

	var buf bytes.Buffer
	buf.Grow(payloadLen)
	_ = buf.WriteByte(op)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(keyBytes)))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(value)))
	_, _ = buf.Write(keyBytes)
	_, _ = buf.Write(value)
	return buf.Bytes()
}

func encodeWALRecord(op byte, key string, value []byte) []byte {
	payload := encodeWALPayload(op, key, value)
	payloadLen := uint32(len(payload))
	crc := crc32.ChecksumIEEE(payload)

	record := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(record[0:4], payloadLen)
	copy(record[4:], payload)
	binary.LittleEndian.PutUint32(record[4+len(payload):], crc)
	return record
}

// decodeWALPayload validates and decodes a single WAL payload.
func decodeWALPayload(payload []byte) (walRecord, error) {
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

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
