package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pengtong/small-db/smalldb"
)

func main() {
	var (
		dirFlag        = flag.String("dir", "", "database directory (default: create a temp dir)")
		keepFlag       = flag.Bool("keep", false, "keep directory on exit (when using a temp dir)")
		backgroundFlag = flag.Bool("background", false, "demonstrate background checkpointing")
	)
	flag.Parse()

	dir := *dirFlag
	createdTemp := false
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "small-db-example-*")
		if err != nil {
			panic(err)
		}
		createdTemp = true
	}
	if createdTemp && !*keepFlag {
		defer os.RemoveAll(dir)
	}

	fmt.Printf("data dir: %s\n", dir)

	opts := smalldb.Options{
		Dir:             dir,
		CreateIfMissing: true,
	}
	if *backgroundFlag {
		opts.CheckpointInterval = 2 * time.Second
		opts.CheckpointUpdates = 5
		opts.CheckpointLogBytes = 4 << 10
	}

	db, err := smalldb.Open(opts)
	if err != nil {
		panic(err)
	}

	if err := db.Set("hello", []byte("world")); err != nil {
		panic(err)
	}
	if err := db.Set("count", []byte("1")); err != nil {
		panic(err)
	}

	if *backgroundFlag {
		start := mustReadVersion(dir)
		for i := 0; i < 50; i++ {
			if err := db.Set("count", []byte(strconv.Itoa(i))); err != nil {
				panic(err)
			}
		}
		if err := waitUntilVersionBumps(dir, start, 6*time.Second); err != nil {
			panic(err)
		}

		fmt.Printf("\nAfter background checkpoint, version=%d\n", mustReadVersion(dir))
		printDir(dir)
	} else {
		fmt.Printf("\nAfter writes (before checkpoint), version=%d\n", mustReadVersion(dir))
		printDir(dir)

		if err := db.Checkpoint(); err != nil {
			panic(err)
		}

		fmt.Printf("\nAfter checkpoint, version=%d\n", mustReadVersion(dir))
		printDir(dir)

		// New writes after checkpoint should go to the new generation's WAL.
		if err := db.Set("count", []byte("2")); err != nil {
			panic(err)
		}
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	db, err = smalldb.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	v, ok, err := db.Get("hello")
	if err != nil {
		panic(err)
	}
	if !ok {
		fmt.Println("hello is missing")
		return
	}
	fmt.Printf("\nAfter restart, hello=%s\n", string(v))

	count, ok, err := db.Get("count")
	if err != nil {
		panic(err)
	}
	if !ok {
		fmt.Println("count is missing")
		return
	}
	fmt.Printf("After restart, count=%s\n", string(count))
}

func mustReadVersion(dir string) uint64 {
	data, err := os.ReadFile(dir + "/version")
	if err != nil {
		panic(err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

func waitUntilVersionBumps(dir string, start uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mustReadVersion(dir) > start {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for version to advance beyond %d", start)
}

func printDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	fmt.Println("files:")
	for _, name := range names {
		fmt.Printf("- %s\n", name)
	}
}
