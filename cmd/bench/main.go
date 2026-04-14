package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/saisushantht/gokv"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	n := flag.Int("n", 100000, "number of operations")
	dir := flag.String("dir", "/tmp/gokv-bench", "data directory")
	flag.Parse()

	os.RemoveAll(*dir)

	cfg := gokv.DefaultConfig(*dir)
	reg := prometheus.NewRegistry()
	db, err := gokv.OpenWithRegistry(cfg, reg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	val := []byte("value-padding-padding-padding-padding")

	// Write benchmark
	start := time.Now()
	for i := 0; i < *n; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		if err := db.Put(key, val); err != nil {
			fmt.Fprintf(os.Stderr, "put: %v\n", err)
			os.Exit(1)
		}
	}
	writeDur := time.Since(start)
	writeOps := float64(*n) / writeDur.Seconds()

	// Read benchmark
	start = time.Now()
	for i := 0; i < *n; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		if _, err := db.Get(key); err != nil {
			fmt.Fprintf(os.Stderr, "get: %v\n", err)
			os.Exit(1)
		}
	}
	readDur := time.Since(start)
	readOps := float64(*n) / readDur.Seconds()

	fmt.Printf("writes: %d ops in %v (%.0f ops/sec)\n", *n, writeDur.Round(time.Millisecond), writeOps)
	fmt.Printf("reads:  %d ops in %v (%.0f ops/sec)\n", *n, readDur.Round(time.Millisecond), readOps)
}