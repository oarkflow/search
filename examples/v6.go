package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	start := time.Now()
	store := New("charge_master.json")
	if err := store.Open(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Indexing took", time.Since(start))
	defer store.Close()
	// full-text lookup
	start = time.Now()
	raws, err := store.Search("31686216")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Searching took", time.Since(start))
	for _, r := range raws {
		fmt.Printf("Found: %+v\n", string(r))
	}
}

var wordRE = regexp.MustCompile(`[_A-Za-z0-9]+`)

// JSONLineStore manages a newline-delimited JSON file + inverted index.
type JSONLineStore struct {
	path    string // path to NDJSON file
	idxPath string // path to index file
	file    *os.File
	mu      sync.RWMutex

	offsets []int64          // byte offsets of each record
	index   map[string][]int // word → list of record indices
	modTime time.Time
}

// New returns a store for either an NDJSON or a JSON-array file.
func New(jsonPath string) *JSONLineStore {
	return &JSONLineStore{
		path:    jsonPath,
		idxPath: jsonPath + ".idx",
		index:   make(map[string][]int),
	}
}

// Open initializes the store: detects format, normalizes to NDJSON, then load/build index.
func (s *JSONLineStore) Open() error {
	// 1) Detect & normalize
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	// peek first non-space
	buf := bufio.NewReader(f)
	first, err := buf.Peek(1)
	if err != nil {
		return err
	}

	// if it's a JSON array, decode whole array → write NDJSON
	if first[0] == '[' {
		dec := json.NewDecoder(buf)
		var arr []json.RawMessage
		if err := dec.Decode(&arr); err != nil {
			return err
		}
		// write to temp NDJSON
		ndPath := s.path + ".ndjson"
		nf, err := os.Create(ndPath)
		if err != nil {
			return err
		}
		for _, raw := range arr {
			nf.Write(raw)
			nf.Write([]byte("\n"))
		}
		nf.Close()

		// switch store to use NDJSON
		s.path = ndPath
		s.idxPath = ndPath + ".idx"
	}

	// 2) Open (or re-open) NDJSON file for read/write
	f2, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	s.file = f2

	info, err := f2.Stat()
	if err != nil {
		return err
	}
	s.modTime = info.ModTime()

	// 3) Try load existing index
	if err := s.loadIndex(); err == nil {
		return nil
	}
	// else build fresh
	return s.buildIndex()
}

// Close the underlying file.
func (s *JSONLineStore) Close() error {
	return s.file.Close()
}

// Write appends an object (marshal→newline) + updates index in O(1).
func (s *JSONLineStore) Write(obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	line := append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	off, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := s.file.Write(line); err != nil {
		return err
	}

	recNum := len(s.offsets)
	s.offsets = append(s.offsets, off)
	s.tokenizeAndIndex(line, recNum)

	info, _ := s.file.Stat()
	s.modTime = info.ModTime()
	return s.saveIndex()
}

// Search returns all raw JSON lines containing term (case-insensitive).
func (s *JSONLineStore) Search(term string) ([]json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := strings.ToLower(term)
	recs, ok := s.index[key]
	if !ok {
		return nil, nil
	}

	out := make([]json.RawMessage, 0, len(recs))
	for _, i := range recs {
		if i < 0 || i >= len(s.offsets) {
			continue
		}
		start := s.offsets[i]
		var end int64
		if i+1 < len(s.offsets) {
			end = s.offsets[i+1]
		} else {
			sz, err := s.file.Stat()
			if err != nil {
				continue
			}
			end = sz.Size()
		}
		buf := make([]byte, end-start)
		if _, err := s.file.ReadAt(buf, start); err != nil {
			continue
		}
		buf = bytes.TrimRight(buf, "\n")
		out = append(out, buf)
	}
	return out, nil
}

// ——— internal ———

func (s *JSONLineStore) tokenizeAndIndex(line []byte, recNum int) {
	words := wordRE.FindAll(line, -1)
	seen := make(map[string]struct{}, len(words))
	for _, w := range words {
		key := strings.ToLower(string(w))
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		s.index[key] = append(s.index[key], recNum)
	}
}

// mmapFile memory-maps the file read-only.
func mmapFile(f *os.File) ([]byte, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(fi.Size())
	if size == 0 {
		return nil, nil
	}
	data, err := syscall.Mmap(
		int(f.Fd()),
		0,
		size,
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// buildIndex scans and indexes the entire NDJSON file efficiently.
func (s *JSONLineStore) buildIndex() error {
	data, err := mmapFile(s.file)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // empty file
	}
	defer syscall.Munmap(data)

	// Split lines
	lines := bytes.Split(data, []byte{'\n'})
	total := len(lines)

	// Trim trailing empty line (if file ends with \n)
	if total > 0 && len(lines[total-1]) == 0 {
		lines = lines[:total-1]
		total--
	}

	// Cap workers to number of lines
	workers := runtime.GOMAXPROCS(0)
	if total < workers {
		workers = total
	}
	chunk := (total + workers - 1) / workers

	s.offsets = make([]int64, total)
	partials := make([]map[string][]int, workers)

	for i := range partials {
		partials[i] = make(map[string][]int)
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	scanWords := func(line []byte, emit func(string)) {
		n := len(line)
		i := 0
		for i < n {
			for i < n && !isWordChar(line[i]) {
				i++
			}
			start := i
			for i < n && isWordChar(line[i]) {
				i++
			}
			if start < i {
				word := toLowerCopy(line[start:i])
				emit(string(word))
			}
		}
	}

	for p := 0; p < workers; p++ {
		lo := p * chunk
		hi := (p + 1) * chunk
		if hi > total {
			hi = total
		}
		go func(p, lo, hi int) {
			defer wg.Done()
			if lo >= total {
				return
			}

			var off int64
			for i := 0; i < lo; i++ {
				off += int64(len(lines[i]) + 1)
			}

			for i := lo; i < hi; i++ {
				s.offsets[i] = off

				seen := make(map[string]struct{})
				scanWords(lines[i], func(w string) {
					if _, ok := seen[w]; ok {
						return
					}
					seen[w] = struct{}{}
					partials[p][w] = append(partials[p][w], i)
				})

				off += int64(len(lines[i]) + 1)
			}
		}(p, lo, hi)
	}

	wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.index = make(map[string][]int)
	for _, part := range partials {
		for word, list := range part {
			s.index[word] = append(s.index[word], list...)
		}
	}

	return s.saveIndex()
}

func isWordChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c == '_'
}

func toLowerCopy(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return out
}

func (s *JSONLineStore) saveIndex() error {
	tmp := s.idxPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(s.modTime); err != nil {
		f.Close()
		return err
	}
	if err := enc.Encode(s.offsets); err != nil {
		f.Close()
		return err
	}
	if err := enc.Encode(s.index); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, s.idxPath)
}

func (s *JSONLineStore) loadIndex() error {
	f, err := os.Open(s.idxPath)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	var mod time.Time
	var offs []int64
	var idx map[string][]int

	if err := dec.Decode(&mod); err != nil {
		return err
	}
	if !mod.Equal(s.modTime) {
		return errors.New("index outdated")
	}
	if err := dec.Decode(&offs); err != nil {
		return err
	}
	if err := dec.Decode(&idx); err != nil {
		return err
	}
	s.offsets = offs
	s.index = idx
	return nil
}
