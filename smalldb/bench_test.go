package smalldb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

// BenchmarkDB_Set measures the performance of write operations.
// Since writes are fsync'd, this is expected to be dominated by disk I/O latency.
func BenchmarkDB_Set(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-set-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir, CreateIfMissing: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	value := []byte("value")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := db.Set(key, value); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}

// BenchmarkDB_Get measures the performance of read operations.
// These are served from memory and should be very fast.
func BenchmarkDB_Get(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir, CreateIfMissing: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate
	const numItems = 1000
	value := []byte("value")
	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := db.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%d", i%numItems)
		if _, _, err := db.Get(key); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkDB_Set_Parallel measures concurrent write performance.
// The DB serializes writes, so this tests lock contention and I/O serialization.
func BenchmarkDB_Set_Parallel(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-set-parallel-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir, CreateIfMissing: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	value := []byte("value")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("k-%d-%d", rand.Int(), i)
			if err := db.Set(key, value); err != nil {
				b.Fatalf("Set: %v", err)
			}
			i++
		}
	})
}

// BenchmarkDB_Get_Parallel measures concurrent read performance.
// The DB uses a RWMutex, so this should scale well with CPU cores.
func BenchmarkDB_Get_Parallel(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-get-parallel-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{Dir: dir, CreateIfMissing: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const numItems = 1000
	value := []byte("value")
	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := db.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("k%d", i%numItems)
			if _, _, err := db.Get(key); err != nil {
				b.Fatalf("Get: %v", err)
			}
			i++
		}
	})
}
