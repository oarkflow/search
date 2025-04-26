package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var wordRE = regexp.MustCompile(`[_A-Za-z0-9]+`)

type numEntry struct {
	Value float64
	Idx   int
}

type Query struct {
	Field    string
	Operator string
	Value    interface{}
}

type JSONLineStore struct {
	path          string
	idxPath       string
	file          *os.File
	mu            sync.RWMutex
	offsets       []int64
	index         map[string][]int
	fieldEqIndex  map[string]map[string][]int
	fieldNumIndex map[string][]numEntry
	checksum      string
}

func New(jsonPath string) *JSONLineStore {
	return &JSONLineStore{
		path:          jsonPath,
		idxPath:       jsonPath + ".idx",
		index:         make(map[string][]int),
		fieldEqIndex:  make(map[string]map[string][]int),
		fieldNumIndex: make(map[string][]numEntry),
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
		if err := json.Unmarshal(bts, &arr); err != nil {
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

func (s *JSONLineStore) Write(obj any) error {
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
	var m map[string]any
	if err := json.Unmarshal(line, &m); err == nil {
		s.indexFields(m, recNum)
	}
	cs, err := computeChecksum(s.file)
	if err != nil {
		return err
	}
	s.checksum = cs
	return s.saveIndex()
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

func (s *JSONLineStore) indexFields(m map[string]any, recNum int) {
	for field, val := range m {
		valStr := fmt.Sprintf("%v", val)
		if s.fieldEqIndex[field] == nil {
			s.fieldEqIndex[field] = make(map[string][]int)
		}
		s.fieldEqIndex[field][valStr] = append(s.fieldEqIndex[field][valStr], recNum)
		switch num := val.(type) {
		case float64:
			s.fieldNumIndex[field] = append(s.fieldNumIndex[field], numEntry{Value: num, Idx: recNum})
		case json.Number:

			if f, err := num.Float64(); err == nil {
				s.fieldNumIndex[field] = append(s.fieldNumIndex[field], numEntry{Value: f, Idx: recNum})
			}
		case int:
			s.fieldNumIndex[field] = append(s.fieldNumIndex[field], numEntry{Value: float64(num), Idx: recNum})

		}
	}
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

func (s *JSONLineStore) SearchQueries(queries ...Query) ([]json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []map[int]struct{}
	for _, q := range queries {
		set := make(map[int]struct{})
		switch q.Operator {
		case "=":
			valStr := fmt.Sprintf("%v", q.Value)
			if recs, ok := s.fieldEqIndex[q.Field][valStr]; ok {
				for _, r := range recs {
					set[r] = struct{}{}
				}
			}
		case "!=":
			match := make(map[int]struct{})
			valStr := fmt.Sprintf("%v", q.Value)
			if recs, ok := s.fieldEqIndex[q.Field][valStr]; ok {
				for _, r := range recs {
					match[r] = struct{}{}
				}
			}
			if fieldMap, ok := s.fieldEqIndex[q.Field]; ok {
				for _, recs := range fieldMap {
					for _, r := range recs {
						if _, found := match[r]; !found {
							set[r] = struct{}{}
						}
					}
				}
			}
		case ">", ">=", "<", "<=":
			numVal, err := toFloat64(q.Value)
			if err != nil {
				return nil, fmt.Errorf("value for range query must be numeric: %v", q.Value)
			}
			entries, ok := s.fieldNumIndex[q.Field]
			if !ok {
				set = map[int]struct{}{}
			} else {
				var start, end int
				switch q.Operator {
				case ">":
					start = sort.Search(len(entries), func(i int) bool {
						return entries[i].Value > numVal
					})
					end = len(entries)
				case ">=":
					start = sort.Search(len(entries), func(i int) bool {
						return entries[i].Value >= numVal
					})
					end = len(entries)
				case "<":
					end = sort.Search(len(entries), func(i int) bool {
						return entries[i].Value >= numVal
					})
					start = 0
				case "<=":
					end = sort.Search(len(entries), func(i int) bool {
						return entries[i].Value > numVal
					})
					start = 0
				}
				for i := start; i < end; i++ {
					set[entries[i].Idx] = struct{}{}
				}
			}
		default:
			return nil, fmt.Errorf("unsupported operator %q", q.Operator)
		}
		results = append(results, set)
	}

	var finalSet map[int]struct{}
	if len(results) > 0 {
		finalSet = results[0]
		for i := 1; i < len(results); i++ {
			finalSet = intersect(finalSet, results[i])
			if len(finalSet) == 0 {
				break
			}
		}
	}

	var recIndices []int
	for idx := range finalSet {
		recIndices = append(recIndices, idx)
	}
	sort.Ints(recIndices)
	var out []json.RawMessage
	for _, i := range recIndices {
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

func toFloat64(v interface{}) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case string:
		return strconv.ParseFloat(t, 64)
	default:
		return 0, fmt.Errorf("cannot convert %v (%T) to float64", v, v)
	}
}

func intersect(a, b map[int]struct{}) map[int]struct{} {
	if len(a) > len(b) {
		a, b = b, a
	}
	res := make(map[int]struct{})
	for k := range a {
		if _, ok := b[k]; ok {
			res[k] = struct{}{}
		}
	}
	return res
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

	type indexResult struct {
		tokenIndex map[string][]int
		fieldEq    map[string]map[string][]int
		fieldNum   map[string][]numEntry
	}

	numWorkers := runtime.GOMAXPROCS(0)
	jobCh := make(chan struct {
		index int
		line  []byte
	}, total)
	resCh := make(chan indexResult, numWorkers)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		partial := indexResult{
			tokenIndex: make(map[string][]int),
			fieldEq:    make(map[string]map[string][]int),
			fieldNum:   make(map[string][]numEntry),
		}
		for job := range jobCh {

			words := wordRE.FindAll(job.line, -1)
			seen := make(map[string]struct{})
			for _, w := range words {
				key := strings.ToLower(string(w))
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				partial.tokenIndex[key] = append(partial.tokenIndex[key], job.index)
			}

			var m map[string]any
			if err := json.Unmarshal(job.line, &m); err == nil {
				for field, val := range m {
					valStr := fmt.Sprintf("%v", val)
					if partial.fieldEq[field] == nil {
						partial.fieldEq[field] = make(map[string][]int)
					}
					partial.fieldEq[field][valStr] = append(partial.fieldEq[field][valStr], job.index)
					switch num := val.(type) {
					case float64:
						partial.fieldNum[field] = append(partial.fieldNum[field], numEntry{Value: num, Idx: job.index})
					case json.Number:
						if f, err := num.Float64(); err == nil {
							partial.fieldNum[field] = append(partial.fieldNum[field], numEntry{Value: f, Idx: job.index})
						}
					case int:
						partial.fieldNum[field] = append(partial.fieldNum[field], numEntry{Value: float64(num), Idx: job.index})
					}
				}
			}
		}
		resCh <- partial
	}

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	for i, line := range lines {

		jobCh <- struct {
			index int
			line  []byte
		}{index: i, line: line}
	}
	close(jobCh)
	wg.Wait()
	close(resCh)

	mergedToken := make(map[string][]int)
	mergedFieldEq := make(map[string]map[string][]int)
	mergedFieldNum := make(map[string][]numEntry)
	for res := range resCh {
		for token, idxs := range res.tokenIndex {
			mergedToken[token] = append(mergedToken[token], idxs...)
		}
		for field, valMap := range res.fieldEq {
			if mergedFieldEq[field] == nil {
				mergedFieldEq[field] = make(map[string][]int)
			}
			for val, idxs := range valMap {
				mergedFieldEq[field][val] = append(mergedFieldEq[field][val], idxs...)
			}
		}
		for field, entries := range res.fieldNum {
			mergedFieldNum[field] = append(mergedFieldNum[field], entries...)
		}
	}

	for field, entries := range mergedFieldNum {
		sort.Slice(entries, func(i, j int) bool { return entries[i].Value < entries[j].Value })
		mergedFieldNum[field] = entries
	}

	s.mu.Lock()
	s.index = mergedToken
	s.fieldEqIndex = mergedFieldEq
	s.fieldNumIndex = mergedFieldNum
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
	if err := enc.Encode(s.fieldEqIndex); err != nil {
		f.Close()
		return err
	}
	if err := enc.Encode(s.fieldNumIndex); err != nil {
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
	var tokenIdx map[string][]int
	var fieldEq map[string]map[string][]int
	var fieldNum map[string][]numEntry
	if err := dec.Decode(&savedCS); err != nil {
		return err
	}
	if savedCS != s.checksum {
		return errors.New("index outdated")
	}
	if err := dec.Decode(&offs); err != nil {
		return err
	}
	if err := dec.Decode(&tokenIdx); err != nil {
		return err
	}
	if err := dec.Decode(&fieldEq); err != nil {
		return err
	}
	if err := dec.Decode(&fieldNum); err != nil {
		return err
	}
	s.offsets = offs
	s.index = tokenIdx
	s.fieldEqIndex = fieldEq
	s.fieldNumIndex = fieldNum
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
	data := map[string]any{
		"charge_amt":           "814.00",
		"charge_type":          "ED_TEST",
		"client_internal_code": "H4020059",
		"client_proc_esc":      nil,
		"cpt_hcpcs_code":       "G0365",
		"created_at":           "2025-04-24T17:44:37.47337824+05:45",
		"id":                   31744045,
		"is_active":            true,
		"physician_category":   nil,
		"status":               "ACTIVE",
		"updated_at":           "2025-04-24T17:44:37.473388778+05:45",
		"work_item_id":         45,
	}
	store.Write(data)
	start = time.Now()
	term := "ED_TEST"
	raws, err := store.Search(term)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Full-text search took", time.Since(start))
	for _, r := range raws {
		fmt.Printf("Found record: %s\n", string(r))
	}
	start = time.Now()
	term = "G0365"
	raws, err = store.Search(term)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Full-text search took", time.Since(start))
	for _, r := range raws {
		fmt.Printf("Found (token %s): %s\n", term, string(r))
	}

	queries := []Query{
		{Field: "is_active", Operator: "=", Value: true},
		{Field: "cpt_hcpcs_code", Operator: "=", Value: "G0365"},
		{Field: "charge_type", Operator: "=", Value: "ED_FACILITY"},
	}
	start = time.Now()
	raws, err = store.SearchQueries(queries...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Field queries took", time.Since(start))
	for _, r := range raws {
		fmt.Printf("Found record: %s\n", string(r))
	}
}
