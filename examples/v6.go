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

	"github.com/oarkflow/filters"
	"github.com/oarkflow/json/jsonmap"
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

type SortField struct {
	Field string
	Desc  bool
}

type SearchOptions struct {
	Queries    []filters.Filter
	SortFields []SortField
	Page       int
	PerPage    int
}

type JSONLineStore struct {
	path           string
	idxPath        string
	file           *os.File
	mu             sync.RWMutex
	saveMu         sync.Mutex
	offsets        []int64
	index          map[string][]int
	fieldEqIndex   map[string]map[string][]int
	fieldNumIndex  map[string][]numEntry
	hasher         hash.Hash
	checksum       string
	pendingWrites  int
	lastSave       time.Time
	writeThreshold int
	flushInterval  time.Duration
	cleanup        bool
}

func New(jsonPath string, reset ...bool) *JSONLineStore {
	var cleanup bool
	if len(reset) > 0 && reset[0] {
		cleanup = true
	}
	return &JSONLineStore{
		path:           jsonPath,
		idxPath:        jsonPath + ".idx",
		cleanup:        cleanup,
		index:          make(map[string][]int),
		fieldEqIndex:   make(map[string]map[string][]int),
		fieldNumIndex:  make(map[string][]numEntry),
		writeThreshold: 100,
		flushInterval:  5 * time.Second,
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
		idxPath := ndPath + ".idx"
		if s.cleanup {
			os.Remove(ndPath)
			os.Remove(ndPath + ".idx")
		}
		nf, err := os.Create(ndPath)
		if err != nil {
			return err
		}
		for _, raw := range arr {
			nf.Write(raw)
			nf.Write([]byte("\n"))
		}
		nf.Close()
		s.path = ndPath
		s.idxPath = idxPath
	}
	f2, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	s.file = f2
	if err := s.computeInitialChecksum(); err != nil {
		s.file.Close()
		return err
	}
	if err := s.loadIndex(); err == nil {
		return nil
	}
	return s.buildIndex()
}

func (s *JSONLineStore) computeInitialChecksum() error {
	s.file.Seek(0, io.SeekStart)
	h := sha256.New()
	io.Copy(h, s.file)
	s.checksum = hex.EncodeToString(h.Sum(nil))
	s.hasher = sha256.New()
	s.hasher.Write(h.Sum(nil))
	s.lastSave = time.Now()
	s.hasher.Reset()
	return nil
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
	s.file.Write(line)
	recNum := len(s.offsets)
	s.offsets = append(s.offsets, off)
	s.tokenizeAndIndex(line, recNum)
	var m map[string]any
	if err := jsonmap.Unmarshal(line, &m); err == nil {
		s.indexFields(m, recNum)
	}
	s.hasher.Write(line)
	s.checksum = hex.EncodeToString(s.hasher.Sum(nil))
	s.pendingWrites++
	if s.pendingWrites >= s.writeThreshold || time.Since(s.lastSave) >= s.flushInterval {
		go s.asyncSaveIndex()
	}
	return nil
}

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
	tmp := s.idxPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Println(err)
		return
	}
	enc := gob.NewEncoder(f)
	enc.Encode(checksum)
	enc.Encode(offsets)
	enc.Encode(index)
	enc.Encode(fieldEq)
	enc.Encode(fieldNum)
	f.Close()
	os.Rename(tmp, s.idxPath)
	s.mu.Lock()
	s.pendingWrites = 0
	s.lastSave = time.Now()
	s.mu.Unlock()
}

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
	seen := map[string]struct{}{}
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

func (s *JSONLineStore) SearchQueries(queries ...filters.Filter) ([]json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var resultIndices []int
	for i := range s.offsets {
		recs, err := s.readRecords([]int{i})
		if err != nil || len(recs) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(recs[0], &m); err != nil {
			continue
		}
		matchAll := true
		for _, f := range queries {
			if !f.Match(m) {
				matchAll = false
				break
			}
		}
		if matchAll {
			resultIndices = append(resultIndices, i)
		}
	}
	sort.Ints(resultIndices)
	return s.readRecords(resultIndices)
}

func (s *JSONLineStore) SearchWithOptions(opts SearchOptions) ([]json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var recIndices []int
	if len(opts.Queries) > 0 {
		for i := range s.offsets {
			recs, err := s.readRecords([]int{i})
			if err != nil || len(recs) == 0 {
				continue
			}
			var m map[string]any
			if err := jsonmap.Unmarshal(recs[0], &m); err != nil {
				continue
			}
			matchAll := true
			for _, f := range opts.Queries {
				if !f.Match(m) {
					matchAll = false
					break
				}
			}
			if matchAll {
				recIndices = append(recIndices, i)
			}
		}
	} else {
		recIndices = make([]int, len(s.offsets))
		for i := range s.offsets {
			recIndices[i] = i
		}
	}
	if len(opts.SortFields) > 0 {
		sort.Slice(recIndices, func(i, j int) bool {
			aIdx := recIndices[i]
			bIdx := recIndices[j]
			aVals := valueAt(aIdx, opts.SortFields, s)
			bVals := valueAt(bIdx, opts.SortFields, s)
			for k, sf := range opts.SortFields {
				cmp := compare(aVals[k], bVals[k])
				if cmp == 0 {
					continue
				}
				if sf.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}
	total := len(recIndices)
	if opts.Page < 1 {
		opts.Page = 1
	}
	start := (opts.Page - 1) * opts.PerPage
	if start >= total {
		return nil, nil
	}
	end := start + opts.PerPage
	if end > total {
		end = total
	}
	return s.readRecords(recIndices[start:end])
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

func intersectAll(sets []map[int]struct{}) map[int]struct{} {
	if len(sets) == 0 {
		return nil
	}
	res := sets[0]
	for _, s2 := range sets[1:] {
		res = intersect(res, s2)
		if len(res) == 0 {
			break
		}
	}
	return res
}

func valueAt(idx int, sortFields []SortField, s *JSONLineStore) []interface{} {
	raws, _ := s.readRecords([]int{idx})
	var m map[string]any
	json.Unmarshal(raws[0], &m)
	vals := make([]interface{}, len(sortFields))
	for i, sf := range sortFields {
		vals[i] = m[sf.Field]
	}
	return vals
}

func compare(a, b interface{}) int {
	switch av := a.(type) {
	case float64:
		if bv, ok := b.(float64); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	return strings.Compare(sa, sb)
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
		s.file.ReadAt(buf, start)
		out = append(out, bytes.TrimRight(buf, "\n"))
	}
	return out, nil
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
	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
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
		local := idxRes{token: make(map[string][]int), field: make(map[string]map[string][]int), num: make(map[string][]numEntry)}
		for job := range jobCh {
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
	dec.Decode(&savedCS)
	if savedCS != s.checksum {
		return errors.New("index outdated")
	}
	dec.Decode(&offs)
	dec.Decode(&tok)
	dec.Decode(&fe)
	dec.Decode(&fn)
	s.offsets = offs
	s.index = tok
	s.fieldEqIndex = fe
	s.fieldNumIndex = fn
	return nil
}

func main() {
	start := time.Now()
	store := New("charge_master.json", true)
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
	queries := []filters.Filter{
		{Field: "is_active", Operator: filters.Equal, Value: true},
		{Field: "cpt_hcpcs_code", Operator: filters.Equal, Value: "G0365"},
		{Field: "charge_type", Operator: filters.Equal, Value: "ED_FACILITY"},
	}
	start = time.Now()
	options := SearchOptions{
		Queries:    queries,
		SortFields: []SortField{{Field: "charge_amt", Desc: true}},
		Page:       1,
		PerPage:    1,
	}
	raws, err = store.SearchWithOptions(options)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Field queries took", time.Since(start))
	for _, r := range raws {
		fmt.Printf("Found record: %s\n", string(r))
	}
}
