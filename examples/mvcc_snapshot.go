//go:build ignore
// +build ignore

package main

import (
	"fmt"
	db "nyxdb/internal/engine"
	"os"
)

func main() {
	opts := db.DefaultOptions
	dir, err := os.MkdirTemp("", "nyxdb-mvcc-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.EnableDiagnostics = true

	engine, err := db.Open(opts)
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Put([]byte("account"), []byte("balance:100")); err != nil {
		panic(err)
	}

	rt := engine.BeginReadTxn()

	if err := engine.Put([]byte("account"), []byte("balance:200")); err != nil {
		panic(err)
	}

	oldValue, err := rt.Get([]byte("account"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("[snapshot] account = %s\n", string(oldValue))

	newValue, err := engine.Get([]byte("account"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("[latest] account = %s\n", string(newValue))

	rt.Close()

	if err := engine.MergeWithOptions(db.MergeOptions{
		Force:              true,
		DiagnosticsContext: "example",
	}); err != nil {
		panic(err)
	}

	val, err := engine.Get([]byte("account"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("[after merge] account = %s\n", string(val))
}
