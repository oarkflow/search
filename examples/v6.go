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
	"hash"
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
	saveMu        sync.Mutex
	offsets       []int64
	index         map[string][]int
	fieldEqIndex  map[string]map[string][]int
	fieldNumIndex map[string][]numEntry

	// incremental checksum
	hasher         hash.Hash
	checksum       string
	pendingWrites  int
	lastSave       time.Time
	writeThreshold int           // e.g. save after this many writes
	flushInterval  time.Duration // or after this duration
}

// New creates a new JSONLineStore.
func New(jsonPath string) *JSONLineStore {
	return &JSONLineStore{
		path:           jsonPath,
		idxPath:        jsonPath + ".idx",
		index:          make(map[string][]int),
		fieldEqIndex:   make(map[string]map[string][]int),
		fieldNumIndex:  make(map[string][]numEntry),
		writeThreshold: 100,             // adjust as needed
		flushInterval:  5 * time.Second, // adjust as needed
	}
}

func (s *JSONLineStore) Open() error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	// JSON array → ndjson conversion
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

	// compute initial checksum and feed hasher
	if err := s.computeInitialChecksum(); err != nil {
		s.file.Close()
		return err
	}

	// try to load existing index
	if err := s.loadIndex(); err == nil {
		return nil
	}
	return s.buildIndex()
}

// computeInitialChecksum reads the entire file once to initialize hasher & checksum
func (s *JSONLineStore) computeInitialChecksum() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(h, s.file); err != nil {
		return err
	}
	s.checksum = hex.EncodeToString(h.Sum(nil))
	// initialize the incremental hasher state
	s.hasher = sha256.New()
	s.hasher.Write(h.Sum(nil))
	s.lastSave = time.Now()
	s.hasher.Reset()
	return nil
}

func (s *JSONLineStore) Close() error {
	return s.file.Close()
}

// Write appends a new JSON record, updates indexes & checksum, and schedules index save.
func (s *JSONLineStore) Write(obj any) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	line := append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	// append to file
	off, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := s.file.Write(line); err != nil {
		return err
	}
	// record offset
	recNum := len(s.offsets)
	s.offsets = append(s.offsets, off)

	// update full-text index
	s.tokenizeAndIndex(line, recNum)
	// update field indexes
	var m map[string]any
	if err := json.Unmarshal(line, &m); err == nil {
		s.indexFields(m, recNum)
	}

	// update incremental checksum
	s.hasher.Write(line)
	s.checksum = hex.EncodeToString(s.hasher.Sum(nil))

	// schedule index save (batched)
	s.pendingWrites++
	if s.pendingWrites >= s.writeThreshold || time.Since(s.lastSave) >= s.flushInterval {
		go s.asyncSaveIndex()
	}

	return nil
}

// asyncSaveIndex ensures only one concurrent save, and resets counters.
func (s *JSONLineStore) asyncSaveIndex() {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()

	s.mu.RLock()
	checksum := s.checksum
	offsets := append([]int64(nil), s.offsets...)
	index := copyIndex(s.index)
	fieldEq := copyFieldEq(s.fieldEqIndex)
	fieldNum := copyFieldNum(s.fieldNumIndex)
	s.mu.RUnlock()

	// write out to .idx.tmp then rename
	tmp := s.idxPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Println("saveIndex:", err)
		return
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(checksum); err != nil {
		log.Println("saveIndex:", err)
		f.Close()
		return
	}
	if err := enc.Encode(offsets); err != nil {
		log.Println("saveIndex:", err)
		f.Close()
		return
	}
	if err := enc.Encode(index); err != nil {
		log.Println("saveIndex:", err)
		f.Close()
		return
	}
	if err := enc.Encode(fieldEq); err != nil {
		log.Println("saveIndex:", err)
		f.Close()
		return
	}
	if err := enc.Encode(fieldNum); err != nil {
		log.Println("saveIndex:", err)
		f.Close()
		return
	}
	f.Close()
	if err := os.Rename(tmp, s.idxPath); err != nil {
		log.Println("saveIndex:", err)
		return
	}

	s.mu.Lock()
	s.pendingWrites = 0
	s.lastSave = time.Now()
	s.mu.Unlock()
}

// helpers to deep-copy indexes before saving
func copyIndex(orig map[string][]int) map[string][]int {
	dst := make(map[string][]int, len(orig))
	for k, v := range orig {
		dst[k] = append([]int(nil), v...)
	}
	return dst
}
func copyFieldEq(orig map[string]map[string][]int) map[string]map[string][]int {
	dst := make(map[string]map[string][]int, len(orig))
	for f, mp := range orig {
		sub := make(map[string][]int, len(mp))
		for val, idxs := range mp {
			sub[val] = append([]int(nil), idxs...)
		}
		dst[f] = sub
	}
	return dst
}
func copyFieldNum(orig map[string][]numEntry) map[string][]numEntry {
	dst := make(map[string][]numEntry, len(orig))
	for f, entries := range orig {
		dst[f] = append([]numEntry(nil), entries...)
	}
	return dst
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
		// immediately re-sort this field's numeric index to stay correct
		sort.Slice(s.fieldNumIndex[field], func(i, j int) bool {
			return s.fieldNumIndex[field][i].Value < s.fieldNumIndex[field][j].Value
		})
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
	return s.readRecords(recs)
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
			for _, r := range s.fieldEqIndex[q.Field][valStr] {
				set[r] = struct{}{}
			}
		case "!=":
			match := make(map[int]struct{})
			valStr := fmt.Sprintf("%v", q.Value)
			for _, r := range s.fieldEqIndex[q.Field][valStr] {
				match[r] = struct{}{}
			}
			for _, recs := range s.fieldEqIndex[q.Field] {
				for _, r := range recs {
					if _, found := match[r]; !found {
						set[r] = struct{}{}
					}
				}
			}
		case ">", ">=", "<", "<=":
			// numeric range
			numVal, err := toFloat64(q.Value)
			if err != nil {
				return nil, fmt.Errorf("value for range query must be numeric: %v", q.Value)
			}
			entries := s.fieldNumIndex[q.Field]
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
				start = 0
				end = sort.Search(len(entries), func(i int) bool {
					return entries[i].Value >= numVal
				})
			case "<=":
				start = 0
				end = sort.Search(len(entries), func(i int) bool {
					return entries[i].Value > numVal
				})
			}
			for i := start; i < end; i++ {
				set[entries[i].Idx] = struct{}{}
			}
		default:
			return nil, fmt.Errorf("unsupported operator %q", q.Operator)
		}
		results = append(results, set)
	}

	// intersect result sets
	final := results[0]
	for _, s2 := range results[1:] {
		final = intersect(final, s2)
		if len(final) == 0 {
			break
		}
	}

	var recIndices []int
	for idx := range final {
		recIndices = append(recIndices, idx)
	}
	sort.Ints(recIndices)
	return s.readRecords(recIndices)
}

// readRecords loads the raw JSON lines for a set of offsets
func (s *JSONLineStore) readRecords(recs []int) ([]json.RawMessage, error) {
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
		out = append(out, bytes.TrimRight(buf, "\n"))
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

// mmapFile remains unchanged but is now in its own function
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
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}

	s.offsets = make([]int64, len(lines))
	var off int64
	for i, line := range lines {
		s.offsets[i] = off
		off += int64(len(line)) + 1
	}

	// concurrent indexing
	type idxRes struct {
		token map[string][]int
		field map[string]map[string][]int
		num   map[string][]numEntry
	}
	numWorkers := runtime.GOMAXPROCS(0)
	jobCh := make(chan struct {
		i    int
		line []byte
	}, len(lines))
	resCh := make(chan idxRes, numWorkers)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		local := idxRes{
			token: make(map[string][]int),
			field: make(map[string]map[string][]int),
			num:   make(map[string][]numEntry),
		}
		for job := range jobCh {
			// tokenize
			words := wordRE.FindAll(job.line, -1)
			seen := make(map[string]struct{})
			for _, w := range words {
				k := strings.ToLower(string(w))
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = struct{}{}
				local.token[k] = append(local.token[k], job.i)
			}
			// fields
			var m map[string]any
			if err := json.Unmarshal(job.line, &m); err == nil {
				for f, v := range m {
					vs := fmt.Sprintf("%v", v)
					if local.field[f] == nil {
						local.field[f] = make(map[string][]int)
					}
					local.field[f][vs] = append(local.field[f][vs], job.i)
					switch num := v.(type) {
					case float64:
						local.num[f] = append(local.num[f], numEntry{Value: num, Idx: job.i})
					case json.Number:
						if fv, err := num.Float64(); err == nil {
							local.num[f] = append(local.num[f], numEntry{Value: fv, Idx: job.i})
						}
					case int:
						local.num[f] = append(local.num[f], numEntry{Value: float64(num), Idx: job.i})
					}
				}
			}
		}
		resCh <- local
	}

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker()
	}
	for i, line := range lines {
		jobCh <- struct {
			i    int
			line []byte
		}{i, line}
	}
	close(jobCh)
	wg.Wait()
	close(resCh)

	// merge
	mergedTok := make(map[string][]int)
	mergedField := make(map[string]map[string][]int)
	mergedNum := make(map[string][]numEntry)
	for r := range resCh {
		for tok, idxs := range r.token {
			mergedTok[tok] = append(mergedTok[tok], idxs...)
		}
		for f, mp := range r.field {
			if mergedField[f] == nil {
				mergedField[f] = make(map[string][]int)
			}
			for vs, idxs := range mp {
				mergedField[f][vs] = append(mergedField[f][vs], idxs...)
			}
		}
		for f, ents := range r.num {
			mergedNum[f] = append(mergedNum[f], ents...)
		}
	}
	// sort numeric indexes
	for f := range mergedNum {
		sort.Slice(mergedNum[f], func(i, j int) bool {
			return mergedNum[f][i].Value < mergedNum[f][j].Value
		})
	}

	s.mu.Lock()
	s.index = mergedTok
	s.fieldEqIndex = mergedField
	s.fieldNumIndex = mergedNum
	s.mu.Unlock()
	s.asyncSaveIndex()
	return nil
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
	var tok map[string][]int
	var fe map[string]map[string][]int
	var fn map[string][]numEntry

	if err := dec.Decode(&savedCS); err != nil {
		return err
	}
	if savedCS != s.checksum {
		return errors.New("index outdated")
	}
	if err := dec.Decode(&offs); err != nil {
		return err
	}
	if err := dec.Decode(&tok); err != nil {
		return err
	}
	if err := dec.Decode(&fe); err != nil {
		return err
	}
	if err := dec.Decode(&fn); err != nil {
		return err
	}

	s.offsets = offs
	s.index = tok
	s.fieldEqIndex = fe
	s.fieldNumIndex = fn
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
