package smalldb

import (
	"bytes"
	"fmt"
	"testing"
)

func TestDB_BasicOperations(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CreateIfMissing: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Get missing key
	if _, ok, err := db.Get("missing"); err != nil {
		t.Fatalf("Get(missing): %v", err)
	} else if ok {
		t.Fatalf("expected missing key to be not found")
	}

	// Set
	if err := db.Set("key1", []byte("val1")); err != nil {
		t.Fatalf("Set(key1): %v", err)
	}

	// Get
	if val, ok, err := db.Get("key1"); err != nil {
		t.Fatalf("Get(key1): %v", err)
	} else if !ok {
		t.Fatalf("key1 not found")
	} else if string(val) != "val1" {
		t.Errorf("got val %q, want val1", val)
	}

	// Delete
	if err := db.Delete("key1"); err != nil {
		t.Fatalf("Delete(key1): %v", err)
	}

	// Get deleted
	if _, ok, err := db.Get("key1"); err != nil {
		t.Fatalf("Get(key1) after delete: %v", err)
	} else if ok {
		t.Fatalf("key1 found after delete")
	}
}

func TestDB_Recovery(t *testing.T) {
	dir := t.TempDir()

	// 1. Open and write some data
	func() {
		db, err := Open(Options{Dir: dir, CreateIfMissing: true})
		if err != nil {
			t.Fatalf("Open 1: %v", err)
		}
		defer db.Close()

		if err := db.Set("a", []byte("1")); err != nil {
			t.Fatalf("Set a: %v", err)
		}
		if err := db.Set("b", []byte("2")); err != nil {
			t.Fatalf("Set b: %v", err)
		}
	}()

	// 2. Re-open and verify
	func() {
		db, err := Open(Options{Dir: dir})
		if err != nil {
			t.Fatalf("Open 2: %v", err)
		}
		defer db.Close()

		val, ok, err := db.Get("a")
		if err != nil {
			t.Fatalf("Get a: %v", err)
		}
		if !ok || string(val) != "1" {
			t.Errorf("Get a: got %q %v, want 1", val, ok)
		}

		val, ok, err = db.Get("b")
		if err != nil {
			t.Fatalf("Get b: %v", err)
		}
		if !ok || string(val) != "2" {
			t.Errorf("Get b: got %q %v, want 2", val, ok)
		}
	}()
}

func TestDB_Checkpoint(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CreateIfMissing: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write data
	entries := 10
	for i := 0; i < entries; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := db.Set(key, []byte(val)); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	// Trigger Checkpoint
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify checkpoint file exists and version matches.
	// We expect version 1 because initial version is 0.
	v, ok, err := readVersionIfExists(versionPath(dir))
	if err != nil {
		t.Fatalf("readVersionIfExists: %v", err)
	}
	if !ok {
		t.Fatalf("version file missing")
	}
	if v != 1 {
		t.Errorf("expected version 1, got %d", v)
	}

	// Re-open and verify data
	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer db2.Close()

	for i := 0; i < entries; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		got, ok, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if !ok {
			t.Errorf("key %s missing after checkpoint recovery", key)
		} else if !bytes.Equal(got, []byte(val)) {
			t.Errorf("key %s: got %s, want %s", key, got, val)
		}
	}
}