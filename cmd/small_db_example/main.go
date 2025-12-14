package main

import (
	"fmt"

	"github.com/pengtong/small-db/smalldb"
)

func main() {
	db, err := smalldb.Open(smalldb.Options{
		Dir:             "data",
		CreateIfMissing: true,
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Set("hello", []byte("world")); err != nil {
		panic(err)
	}
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
