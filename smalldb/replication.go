package smalldb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync/atomic"
	"time"
)

const (
	replicationAck          byte = 1
	replicationMsgWAL       byte = 0x10
	replicationMsgHeartbeat byte = 0x11
)

// SetBackupConn configures the connection used for primary-to-backup replication.
// Callers should set this before issuing writes.
func (db *DB) SetBackupConn(conn net.Conn) {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.backupConn != nil {
		_ = db.backupConn.Close()
	}
	db.backupConn = conn
}

// SendHeartbeat sends a replication heartbeat if a backup is connected.
func (db *DB) SendHeartbeat() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.closed {
		return errors.New("db is closed")
	}
	if db.backupConn == nil {
		return nil
	}
	if err := writeReplicationMessage(db.backupConn, replicationMsgHeartbeat, nil); err != nil {
		return fmt.Errorf("replication heartbeat: %w", err)
	}
	return nil
}

// ReceiveReplication reads WAL records from conn and applies them locally,
// acknowledging each record after it is durable and applied.
func (db *DB) ReceiveReplication(conn net.Conn, heartbeatTimeout time.Duration) error {
	if conn == nil {
		return errors.New("replication conn is nil")
	}
	defer conn.Close()

	var lastHeard atomic.Int64
	lastHeard.Store(time.Now().UnixNano())

	done := make(chan struct{})
	if heartbeatTimeout > 0 {
		go func() {
			interval := heartbeatTimeout / 2
			if interval <= 0 {
				interval = heartbeatTimeout
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
				}
				last := time.Unix(0, lastHeard.Load())
				if time.Since(last) > heartbeatTimeout {
					_ = conn.Close()
					return
				}
			}
		}()
	}
	defer close(done)

	for {
		msgType, rec, err := readReplicationMessage(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return io.EOF
			}
			return err
		}
		lastHeard.Store(time.Now().UnixNano())
		if msgType == replicationMsgHeartbeat {
			continue
		}
		if err := db.applyReplicaRecord(rec.op, rec.key, rec.value); err != nil {
			return err
		}
		if err := writeReplicationAck(conn); err != nil {
			return err
		}
	}
}

func (db *DB) replicateBatch(batch []writeRequest) error {
	if db.backupConn == nil {
		return nil
	}

	for _, req := range batch {
		recordBytes := encodeWALRecord(req.op, req.key, req.value)
		if err := writeReplicationMessage(db.backupConn, replicationMsgWAL, recordBytes); err != nil {
			return fmt.Errorf("replicate write: %w", err)
		}
	}

	for range batch {
		if err := readReplicationAck(db.backupConn); err != nil {
			return err
		}
	}

	return nil
}

func writeReplicationMessage(w io.Writer, msgType byte, payload []byte) error {
	if err := writeAll(w, []byte{msgType}); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	return writeAll(w, payload)
}

func readReplicationMessage(r io.Reader) (byte, walRecord, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, walRecord{}, err
	}
	switch buf[0] {
	case replicationMsgWAL:
		rec, err := readWALRecord(r)
		return replicationMsgWAL, rec, err
	case replicationMsgHeartbeat:
		return replicationMsgHeartbeat, walRecord{}, nil
	default:
		return 0, walRecord{}, fmt.Errorf("unknown replication message type: %d", buf[0])
	}
}

func (db *DB) applyReplicaRecord(op byte, key string, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.closed || db.wal == nil {
		return errors.New("db is closed")
	}

	n, err := db.wal.writeRecord(op, key, value)
	if err != nil {
		return fmt.Errorf("replica wal write: %w", err)
	}
	if err := db.wal.Sync(); err != nil {
		return fmt.Errorf("replica wal sync: %w", err)
	}

	db.stateMu.Lock()
	applyErr := applyRecord(db.state, walRecord{op: op, key: key, value: value})
	db.stateMu.Unlock()
	if applyErr != nil {
		return applyErr
	}

	db.noteCommittedUpdate(int64(n))
	return nil
}

func readReplicationAck(r io.Reader) error {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return fmt.Errorf("read replication ack: %w", err)
	}
	if buf[0] != replicationAck {
		return fmt.Errorf("invalid replication ack: %d", buf[0])
	}
	return nil
}

func writeReplicationAck(w io.Writer) error {
	if _, err := w.Write([]byte{replicationAck}); err != nil {
		return fmt.Errorf("write replication ack: %w", err)
	}
	return nil
}

func readWALRecord(r io.Reader) (walRecord, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return walRecord{}, err
	}
	payloadLen := binary.LittleEndian.Uint32(lenBuf[:])
	if payloadLen == 0 || payloadLen > 64<<20 {
		return walRecord{}, fmt.Errorf("invalid wal payload length: %d", payloadLen)
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return walRecord{}, err
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return walRecord{}, err
	}
	wantCRC := binary.LittleEndian.Uint32(crcBuf[:])
	gotCRC := crc32.ChecksumIEEE(payload)
	if gotCRC != wantCRC {
		return walRecord{}, fmt.Errorf("wal crc mismatch: want=%d got=%d", wantCRC, gotCRC)
	}
	return decodeWALPayload(payload)
}
