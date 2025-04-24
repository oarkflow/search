package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Document represents one JSON object.
type Document map[string]interface{}

// Index is a sharded in-memory inverted index.
type Index struct {
	shards  []map[string][]int
	locks   []sync.RWMutex
	nShards int
}

// NewIndex allocates nShards empty posting-list maps.
func NewIndex(nShards int) *Index {
	shards := make([]map[string][]int, nShards)
	locks := make([]sync.RWMutex, nShards)
	for i := range shards {
		shards[i] = make(map[string][]int)
	}
	return &Index{shards: shards, locks: locks, nShards: nShards}
}

// shardFor hashes a term into [0, nShards).
func (idx *Index) shardFor(term string) int {
	sum := 0
	for i := 0; i < len(term); i++ {
		sum += int(term[i])
	}
	return sum % idx.nShards
}

// batchFlush merges one worker’s local postings into the global index.
func (idx *Index) batchFlush(local map[string][]int) {
	perShard := make([]map[string][]int, idx.nShards)
	for i := range perShard {
		perShard[i] = make(map[string][]int)
	}
	for term, ids := range local {
		sh := idx.shardFor(term)
		perShard[sh][term] = ids
	}
	for sh, bucket := range perShard {
		if len(bucket) == 0 {
			continue
		}
		idx.locks[sh].Lock()
		for term, ids := range bucket {
			idx.shards[sh][term] = append(idx.shards[sh][term], ids...)
		}
		idx.locks[sh].Unlock()
	}
}

// tokenize lowercases and splits on non-alphanumeric.
func tokenize(s string) []string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune(' ')
		}
	}
	return strings.Fields(b.String())
}

func main() {
	// Flags
	filePath := flag.String("file", "charge_master.json", "path to JSON array file")
	workers := flag.Int("workers", runtime.NumCPU(), "number of indexing workers")
	batchSize := flag.Int("batch", 500, "docs per batch flush")
	flag.Parse()

	// Open file + buffered reader
	f, err := os.Open(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 4<<20) // 4 MiB buffer

	// JSON decoder for a top-level array
	dec := json.NewDecoder(reader)
	tok, err := dec.Token()
	if err != nil {
		fmt.Fprintf(os.Stderr, "json token error: %v\n", err)
		os.Exit(1)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		fmt.Fprintln(os.Stderr, "expected JSON array (‘[’) at top level")
		os.Exit(1)
	}

	// Channel of decoded docs + their ID
	type work struct {
		doc   Document
		docID int
	}
	docCh := make(chan work, *workers*2)

	var totalDocs int64

	// Decoder goroutine
	go func() {
		id := 0
		for dec.More() {
			var doc Document
			if err := dec.Decode(&doc); err != nil {
				continue
			}
			atomic.AddInt64(&totalDocs, 1)
			docCh <- work{doc: doc, docID: id}
			id++
		}
		close(docCh)
	}()

	// Build index
	idx := NewIndex(*workers * 2)
	var wg sync.WaitGroup
	start := time.Now()

	// Worker pool
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make(map[string][]int, *batchSize*4)
			count := 0

			for w := range docCh {
				for _, v := range w.doc {
					if str, ok := v.(string); ok {
						for _, term := range tokenize(str) {
							local[term] = append(local[term], w.docID)
						}
					}
				}
				count++
				if count >= *batchSize {
					idx.batchFlush(local)
					local = make(map[string][]int, *batchSize*4)
					count = 0
				}
			}
			if count > 0 {
				idx.batchFlush(local)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Consume closing ‘]’
	if _, err := dec.Token(); err != nil && err != io.EOF {
		// ignore
	}

	// Print total documents indexed
	fmt.Printf("Indexed %d documents in %v\n", atomic.LoadInt64(&totalDocs), elapsed)

	// Simple REPL
	repl := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("search> ")
		q, _ := repl.ReadString('\n')
		q = strings.TrimSpace(q)
		if q == "exit" || q == "quit" {
			break
		}
		term := strings.ToLower(q)
		sh := idx.shardFor(term)
		idx.locks[sh].RLock()
		hits := idx.shards[sh][term]
		idx.locks[sh].RUnlock()
		if len(hits) == 0 {
			fmt.Println("No matches")
		} else {
			fmt.Printf("Docs: %v\n", hits)
		}
	}
}
