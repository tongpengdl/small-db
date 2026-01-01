package smalldb

import (
	"fmt"
	"io"
	"net"
)

const replicationAck byte = 1

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

func (db *DB) replicateBatch(batch []writeRequest) error {
	if db.backupConn == nil {
		return nil
	}

	for _, req := range batch {
		recordBytes := encodeWALRecord(req.op, req.key, req.value)
		if err := writeAll(db.backupConn, recordBytes); err != nil {
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
