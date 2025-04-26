package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
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

	"github.com/oarkflow/json"
	"github.com/oarkflow/json/jsonmap"
)

var wordRE = regexp.MustCompile(`[_A-Za-z0-9]+`)

type JSONLineStore struct {
	path     string
	idxPath  string
	file     *os.File
	mu       sync.RWMutex
	offsets  []int64
	index    map[string][]int
	checksum string
}

func New(jsonPath string) *JSONLineStore {
	return &JSONLineStore{
		path:    jsonPath,
		idxPath: jsonPath + ".idx",
		index:   make(map[string][]int),
	}
}

func (s *JSONLineStore) Open() error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewReader(f)
	first, err := buf.Peek(1)
	if err != nil {
		return err
	}
	if first[0] == '[' {
		bts, err := io.ReadAll(buf)
		if err != nil {
			return err
		}
		var arr []json.RawMessage
		if err := jsonmap.Unmarshal(bts, &arr); err != nil {
			return err
		}
		ndPath := s.path + ".ndjson"
		nf, err := os.Create(ndPath)
		if err != nil {
			return err
		}
		defer nf.Close()
		for _, raw := range arr {
			nf.Write(raw)
			nf.Write([]byte("\n"))
		}
		s.path = ndPath
		s.idxPath = ndPath + ".idx"
	}

	f2, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	s.file = f2

	cs, err := computeChecksum(s.file)
	if err != nil {
		s.file.Close()
		return err
	}
	s.checksum = cs

	if err := s.loadIndex(); err == nil {
		return nil
	}
	return s.buildIndex()
}

func (s *JSONLineStore) Close() error {
	return s.file.Close()
}

func (s *JSONLineStore) Write(obj interface{}) error {
	data, err := jsonmap.Marshal(obj)
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

	cs, err := computeChecksum(s.file)
	if err != nil {
		return err
	}
	s.checksum = cs
	return s.saveIndex()
}

func (s *JSONLineStore) Search(term string) ([]json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := strings.ToLower(term)
	recs, ok := s.index[key]
	if !ok {
		return nil, nil
	}
	var out []json.RawMessage
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

func computeChecksum(f *os.File) (string, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

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

func (s *JSONLineStore) buildIndex() error {
	data, err := mmapFile(s.file)
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}
	defer syscall.Munmap(data)

	lines := bytes.Split(data, []byte{'\n'})
	total := len(lines)

	if total > 0 && len(lines[total-1]) == 0 {
		lines = lines[:total-1]
		total = len(lines)
	}

	s.offsets = make([]int64, total)
	var off int64
	for i, line := range lines {
		s.offsets[i] = off
		off += int64(len(line)) + 1
	}

	type tokenResult struct {
		tokens map[string][]int
	}
	numWorkers := runtime.GOMAXPROCS(0)
	jobCh := make(chan struct {
		index int
		line  []byte
	})
	resCh := make(chan tokenResult, numWorkers)

	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		partial := make(map[string][]int)
		for job := range jobCh {
			seen := make(map[string]struct{})

			words := wordRE.FindAll(job.line, -1)
			for _, w := range words {
				key := strings.ToLower(string(w))
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				partial[key] = append(partial[key], job.index)
			}
		}
		resCh <- tokenResult{tokens: partial}
	}

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	go func() {
		for i, line := range lines {
			jobCh <- struct {
				index int
				line  []byte
			}{index: i, line: line}
		}
		close(jobCh)
	}()

	wg.Wait()
	close(resCh)

	merged := make(map[string][]int)
	for res := range resCh {
		for token, indices := range res.tokens {
			merged[token] = append(merged[token], indices...)
		}
	}

	s.mu.Lock()
	s.index = merged
	s.mu.Unlock()
	return s.saveIndex()
}

func (s *JSONLineStore) saveIndex() error {
	tmp := s.idxPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(s.checksum); err != nil {
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
	var savedCS string
	var offs []int64
	var idx map[string][]int
	if err := dec.Decode(&savedCS); err != nil {
		return err
	}
	if savedCS != s.checksum {
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

func main() {
	start := time.Now()
	store := New("charge_master.json")
	if err := store.Open(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Indexing took", time.Since(start))
	defer store.Close()

	start = time.Now()
	term := "G0365"
	raws, err := store.Search(term)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Searching took", time.Since(start))
	for _, r := range raws {
		fmt.Printf("Found %s: %s\n", term, string(r))
	}
}
