package main

import (
	"fmt"
	"os"
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
	defer os.RemoveAll(dir)

	fmt.Printf("data dir: %s\n", dir)

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
	if err := db.Close(); err != nil {
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
