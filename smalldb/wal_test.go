package smalldb

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func TestReplayWAL_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := logPath(dir, 0)

	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	if err := w.appendSet("a", []byte("1")); err != nil {
		t.Fatalf("appendSet(a): %v", err)
	}
	if err := w.appendSet("b", []byte{0, 1, 2}); err != nil {
		t.Fatalf("appendSet(b): %v", err)
	}
	if err := w.appendDelete("a"); err != nil {
		t.Fatalf("appendDelete(a): %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	state := make(map[string][]byte)
	if err := replayWAL(path, func(r walRecord) error {
		switch r.op {
		case opSet:
			v := make([]byte, len(r.value))
			copy(v, r.value)
			state[r.key] = v
		case opDelete:
			delete(state, r.key)
		default:
			t.Fatalf("unexpected op: %d", r.op)
		}
		return nil
	}); err != nil {
		t.Fatalf("replayWAL: %v", err)
	}

	if _, ok := state["a"]; ok {
		t.Fatalf("expected key a to be deleted")
	}
	if got, ok := state["b"]; !ok || !bytes.Equal(got, []byte{0, 1, 2}) {
		t.Fatalf("unexpected value for b: ok=%v value=%v", ok, got)
	}
}

func TestReplayWAL_IgnoresTrailingPartialRecord(t *testing.T) {
	dir := t.TempDir()
	path := logPath(dir, 0)

	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	if err := w.appendSet("a", []byte("1")); err != nil {
		t.Fatalf("appendSet: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Append an incomplete record: length prefix says 10 bytes, but we only write 2.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open wal file for append: %v", err)
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], 10)
	if _, err := f.Write(lenBuf[:]); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := f.Write([]byte{0x99, 0x88}); err != nil {
		t.Fatalf("write partial payload: %v", err)
	}
	_ = f.Close()

	var got walRecord
	var gotCount int
	if err := replayWAL(path, func(r walRecord) error {
		got = r
		gotCount++
		return nil
	}); err != nil {
		t.Fatalf("replayWAL: %v", err)
	}

	if gotCount != 1 {
		t.Fatalf("expected 1 record, got %d", gotCount)
	}
	if got.op != opSet || got.key != "a" || !bytes.Equal(got.value, []byte("1")) {
		t.Fatalf("unexpected record: %+v", got)
	}
}

func TestReplayWAL_InvalidLengthErrors(t *testing.T) {
	dir := t.TempDir()
	path := logPath(dir, 0)

	// payloadLen == 0 is rejected.
	if err := os.WriteFile(path, []byte{0, 0, 0, 0}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := replayWAL(path, func(walRecord) error { return nil }); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestReplayWAL_CRCMismatchErrors(t *testing.T) {
	dir := t.TempDir()
	path := logPath(dir, 0)

	// Write a syntactically valid record with an invalid CRC.
	payload := encodeWALPayload(opSet, "k", []byte("v"))

	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(payload)))
	_, _ = buf.Write(payload)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0xdeadbeef))

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := replayWAL(path, func(walRecord) error { return nil }); err == nil {
		t.Fatalf("expected error, got nil")
	}
}
