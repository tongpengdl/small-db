package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/pengtong/small-db/smalldb"
)

func main() {
	dir, err := os.MkdirTemp("", "small-db-example-*")
	if err != nil {
		panic(err)
	}
	// defer os.RemoveAll(dir)

	db, err := smalldb.Open(smalldb.Options{
		Dir:             dir,
		CreateIfMissing: true,
	})
	if err != nil {
		panic(err)
	}

	if err := db.Set("hello", []byte("world")); err != nil {
		panic(err)
	}
	if err := db.Set("count", []byte("1")); err != nil {
		panic(err)
	}

	if err := db.Checkpoint(); err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	fmt.Printf("data dir: %s\nfiles:\n", dir)
	for _, name := range names {
		fmt.Printf("- %s\n", name)
	}

	// Next change will atomically switch to the new generation after checkpointing.
	// For now, we manually bump `version` so the next Open() loads
	// checkpoint.(N+1) and replays logfile.(N+1).
	versionPath := filepath.Join(dir, "version")
	versionData, err := os.ReadFile(versionPath)
	if err != nil {
		panic(err)
	}
	curVersion, err := strconv.ParseUint(strings.TrimSpace(string(versionData)), 10, 64)
	if err != nil {
		panic(err)
	}
	nextVersion := curVersion + 1
	if err := os.WriteFile(versionPath, []byte(strconv.FormatUint(nextVersion, 10)+"\n"), 0o644); err != nil {
		panic(err)
	}

	db, err = smalldb.Open(smalldb.Options{
		Dir:             dir,
		CreateIfMissing: true,
	})
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
	fmt.Printf("hello=%s\n", string(v))
}
