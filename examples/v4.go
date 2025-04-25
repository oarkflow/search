package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	bloomSize = 1 << 16
	k1        = 1.5
	b         = 0.75
)

type Document struct {
	Key       string
	Value     any
	Timestamp int64
}

func documentValueToString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}

type BloomFilter struct {
	size uint
	bits []bool
	lock sync.Mutex
}

func NewBloomFilter(size uint) *BloomFilter {
	return &BloomFilter{
		size: size,
		bits: make([]bool, size),
	}
}

func (bf *BloomFilter) hash(item string, seed uint32) uint {
	h := fnv.New32a()
	h.Write([]byte(item))
	sum := h.Sum32() ^ seed
	return uint(sum) % bf.size
}

func (bf *BloomFilter) Add(item string) {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	bf.bits[bf.hash(item, 0xA3B1)] = true
	bf.bits[bf.hash(item, 0xF1C2)] = true
}

func (bf *BloomFilter) Test(item string) bool {
	bf.lock.Lock()
	defer bf.lock.Unlock()
	return bf.bits[bf.hash(item, 0xA3B1)] && bf.bits[bf.hash(item, 0xF1C2)]
}

type KVStore struct {
	dir             string
	memtable        map[string]Document
	memtableLock    sync.RWMutex
	walFile         *os.File
	walLock         sync.Mutex
	valueLogFile    *os.File
	valueLogLock    sync.Mutex
	stopCompaction  chan struct{}
	compactionWG    sync.WaitGroup
	bloom           *BloomFilter
	writeCounter    int64
	writeCounterMux sync.Mutex
	invertedIndex   map[string]map[string][]int
	indexLock       sync.RWMutex
}

func NewKVStore(dir string) (*KVStore, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, "wal.log")
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	valuelogPath := filepath.Join(dir, "valuelog.dat")
	valLog, err := os.OpenFile(valuelogPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	store := &KVStore{
		dir:            dir,
		memtable:       make(map[string]Document),
		walFile:        walFile,
		valueLogFile:   valLog,
		stopCompaction: make(chan struct{}),
		bloom:          NewBloomFilter(bloomSize),
		invertedIndex:  make(map[string]map[string][]int),
	}

	store.loadWAL()
	store.compactionWG.Add(1)
	go store.compactionLoop()
	return store, nil
}

func (kv *KVStore) Close() error {
	close(kv.stopCompaction)
	kv.compactionWG.Wait()
	if err := kv.walFile.Close(); err != nil {
		return err
	}
	if err := kv.valueLogFile.Close(); err != nil {
		return err
	}
	return nil
}

func (kv *KVStore) loadWAL() {
	kv.walLock.Lock()
	defer kv.walLock.Unlock()
	_, err := kv.walFile.Seek(0, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking WAL:", err)
		return
	}
	dec := gob.NewDecoder(kv.walFile)
	for {
		var doc Document
		err := dec.Decode(&doc)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error decoding WAL:", err)
			break
		}
		kv.memtableLock.Lock()
		if doc.Value == nil {
			delete(kv.memtable, doc.Key)
		} else {
			kv.memtable[doc.Key] = doc
			kv.bloom.Add(doc.Key)
			kv.indexDocument(doc)
		}
		kv.memtableLock.Unlock()
	}
	_, err = kv.walFile.Seek(0, io.SeekEnd)
	if err != nil {
		fmt.Println("Error seeking WAL to end:", err)
	}
}

func (kv *KVStore) writeWAL(doc Document) error {
	kv.walLock.Lock()
	defer kv.walLock.Unlock()
	enc := gob.NewEncoder(kv.walFile)
	err := enc.Encode(doc)
	if err != nil {
		return err
	}
	kv.writeCounterMux.Lock()
	kv.writeCounter++
	counter := kv.writeCounter
	kv.writeCounterMux.Unlock()

	if counter%10 == 0 {
		if err := kv.walFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (kv *KVStore) writeValueLog(doc Document) error {
	kv.valueLogLock.Lock()
	defer kv.valueLogLock.Unlock()
	enc := gob.NewEncoder(kv.valueLogFile)
	err := enc.Encode(doc)
	if err != nil {
		return err
	}
	return nil
}

func tokenize(text string) []string {
	parts := strings.Fields(strings.ToLower(text))
	return parts
}

func (kv *KVStore) indexDocument(doc Document) {
	combined := doc.Key + " " + documentValueToString(doc.Value)
	tokens := tokenize(combined)
	kv.indexLock.Lock()
	defer kv.indexLock.Unlock()
	for pos, token := range tokens {
		if kv.invertedIndex[token] == nil {
			kv.invertedIndex[token] = make(map[string][]int)
		}
		kv.invertedIndex[token][doc.Key] = append(kv.invertedIndex[token][doc.Key], pos)
	}
}

func (kv *KVStore) removeFromIndex(doc Document) {
	kv.indexLock.Lock()
	defer kv.indexLock.Unlock()
	for token, docMap := range kv.invertedIndex {
		if _, exists := docMap[doc.Key]; exists {
			delete(docMap, doc.Key)

			if len(docMap) == 0 {
				delete(kv.invertedIndex, token)
			}
		}
	}
}

func (kv *KVStore) AddDocument(key string, value any) error {
	doc := Document{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}
	if err := kv.writeWAL(doc); err != nil {
		return err
	}
	if err := kv.writeValueLog(doc); err != nil {
		return err
	}
	kv.memtableLock.Lock()
	kv.memtable[key] = doc
	kv.bloom.Add(key)
	kv.memtableLock.Unlock()
	kv.indexDocument(doc)
	return nil
}

func (kv *KVStore) UpdateDocument(key string, value any) error {
	kv.memtableLock.RLock()
	oldDoc, exists := kv.memtable[key]
	kv.memtableLock.RUnlock()
	if exists {
		kv.removeFromIndex(oldDoc)
	}
	return kv.AddDocument(key, value)
}

func (kv *KVStore) DeleteDocument(key string) error {
	doc := Document{
		Key:       key,
		Value:     nil,
		Timestamp: time.Now().UnixNano(),
	}
	if err := kv.writeWAL(doc); err != nil {
		return err
	}
	if err := kv.writeValueLog(doc); err != nil {
		return err
	}
	kv.memtableLock.Lock()
	if oldDoc, exists := kv.memtable[key]; exists {
		kv.removeFromIndex(oldDoc)
	}
	delete(kv.memtable, key)
	kv.memtableLock.Unlock()
	return nil
}

func (kv *KVStore) AddDocuments(docs []Document) error {
	kv.walLock.Lock()
	defer kv.walLock.Unlock()
	enc := gob.NewEncoder(kv.walFile)
	for _, doc := range docs {
		doc.Timestamp = time.Now().UnixNano()
		if err := enc.Encode(doc); err != nil {
			return err
		}
		if err := kv.writeValueLog(doc); err != nil {
			return err
		}
		kv.memtableLock.Lock()
		kv.memtable[doc.Key] = doc
		kv.bloom.Add(doc.Key)
		kv.memtableLock.Unlock()
		kv.indexDocument(doc)
		kv.writeCounterMux.Lock()
		kv.writeCounter++
		kv.writeCounterMux.Unlock()
	}
	return kv.walFile.Sync()
}

func (kv *KVStore) Search(query string, filters ...Filter) []Document {
	kv.memtableLock.RLock()
	defer kv.memtableLock.RUnlock()
	var results []Document
	lQuery := strings.ToLower(query)
	for key, doc := range kv.memtable {
		if !kv.bloom.Test(key) {
			continue
		}
		text := documentValueToString(doc.Value)
		if strings.Contains(strings.ToLower(key), lQuery) || strings.Contains(strings.ToLower(text), lQuery) {
			if len(filters) > 0 {
				docMap, ok := doc.Value.(map[string]any)
				if !ok {
					continue
				}
				match := true
				for _, f := range filters {
					val, exists := docMap[f.Field]
					if !exists {
						match = false
						break
					}
					if f.Operator == "eq" {
						if fmt.Sprintf("%v", val) != f.Value {
							match = false
							break
						}
					} else {
						match = false
						break
					}
				}
				if !match {
					continue
				}
			}
			results = append(results, doc)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})
	return results
}

func (kv *KVStore) Paginate(docs []Document, page, pageSize int) []Document {
	start := page * pageSize
	if start >= len(docs) {
		return []Document{}
	}
	end := start + pageSize
	if end > len(docs) {
		end = len(docs)
	}
	return docs[start:end]
}

func (kv *KVStore) QueryDocuments(query string, queryType string) []Document {
	if queryType == "prefix" {
		kv.memtableLock.RLock()
		var results []Document
		for key, doc := range kv.memtable {
			if strings.HasPrefix(key, query) {
				results = append(results, doc)
			}
		}
		kv.memtableLock.RUnlock()
		sort.Slice(results, func(i, j int) bool {
			return results[i].Key < results[j].Key
		})
		return results
	} else if queryType == "fuzzy" {
		return kv.Search(query)
	}
	return nil
}

func (kv *KVStore) compactionLoop() {
	defer kv.compactionWG.Done()
	interval := 10 * time.Second
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			kv.compactMemtable()
			kv.writeCounterMux.Lock()
			count := kv.writeCounter
			kv.writeCounter = 0
			kv.writeCounterMux.Unlock()
			if count > 100 {
				interval = 5 * time.Second
			} else {
				interval = 10 * time.Second
			}
			ticker.Reset(interval)
		case <-kv.stopCompaction:
			ticker.Stop()
			return
		}
	}
}

func (kv *KVStore) compactMemtable() {
	kv.memtableLock.RLock()
	docs := make([]Document, 0, len(kv.memtable))
	for _, doc := range kv.memtable {
		docs = append(docs, doc)
	}
	kv.memtableLock.RUnlock()
	if len(docs) == 0 {
		return
	}
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].Key < docs[j].Key
	})
	segmentName := fmt.Sprintf("segment_%d.dat", time.Now().UnixNano())
	segmentPath := filepath.Join(kv.dir, segmentName)
	f, err := os.Create(segmentPath)
	if err != nil {
		fmt.Println("Error creating compaction file:", err)
		return
	}
	enc := gob.NewEncoder(f)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			fmt.Println("Error writing to compaction file:", err)
			f.Close()
			return
		}
	}
	f.Sync()
	f.Close()
}

func (kv *KVStore) TermQuery(term string) []Document {
	term = strings.ToLower(term)
	kv.indexLock.RLock()
	docMap, exists := kv.invertedIndex[term]
	kv.indexLock.RUnlock()
	if !exists {
		fmt.Printf("Token %s not found in index.\n", term)
		return nil
	}
	kv.memtableLock.RLock()
	defer kv.memtableLock.RUnlock()
	var results []Document
	for docKey := range docMap {
		if doc, ok := kv.memtable[docKey]; ok {
			results = append(results, doc)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})
	return results
}

func (kv *KVStore) PhraseQuery(phrase string) []Document {
	tokens := tokenize(phrase)
	if len(tokens) == 0 {
		return nil
	}
	kv.indexLock.RLock()
	candidateDocs, exists := kv.invertedIndex[tokens[0]]
	kv.indexLock.RUnlock()
	if !exists {
		return nil
	}
	var results []Document
	kv.memtableLock.RLock()
	defer kv.memtableLock.RUnlock()
	for docKey, positions := range candidateDocs {
		doc, ok := kv.memtable[docKey]
		if !ok {
			continue
		}
		text := documentValueToString(doc.Value)
		docTokens := tokenize(text)
		found := false
		for _, pos := range positions {
			if pos+len(tokens) > len(docTokens) {
				continue
			}
			match := true
			for i := 0; i < len(tokens); i++ {
				if docTokens[pos+i] != tokens[i] {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}
		if found {
			results = append(results, doc)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})
	return results
}

func (kv *KVStore) BooleanQuery(must, should, mustNot []string) []Document {
	getDocsForTerm := func(term string) map[string]struct{} {
		res := make(map[string]struct{})
		results := kv.TermQuery(term)
		for _, doc := range results {
			res[doc.Key] = struct{}{}
		}
		return res
	}

	candidate := make(map[string]struct{})
	initialized := false

	for _, term := range must {
		docSet := getDocsForTerm(term)
		if !initialized {
			candidate = docSet
			initialized = true
		} else {
			for key := range candidate {
				if _, ok := docSet[key]; !ok {
					delete(candidate, key)
				}
			}
		}
	}

	if !initialized {
		kv.memtableLock.RLock()
		for key := range kv.memtable {
			candidate[key] = struct{}{}
		}
		kv.memtableLock.RUnlock()
	}

	if len(should) > 0 {
		shouldCandidate := make(map[string]struct{})
		for _, term := range should {
			for key := range getDocsForTerm(term) {
				shouldCandidate[key] = struct{}{}
			}
		}
		for key := range candidate {
			if _, ok := shouldCandidate[key]; !ok {
				delete(candidate, key)
			}
		}
	}

	for _, term := range mustNot {
		for key := range getDocsForTerm(term) {
			delete(candidate, key)
		}
	}

	var results []Document
	kv.memtableLock.RLock()
	for key := range candidate {
		if doc, ok := kv.memtable[key]; ok {
			results = append(results, doc)
		}
	}
	kv.memtableLock.RUnlock()
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})
	return results
}

func (kv *KVStore) RegexQuery(pattern string) []Document {
	r, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Println("Invalid regex pattern:", err)
		return nil
	}
	kv.memtableLock.RLock()
	defer kv.memtableLock.RUnlock()
	var results []Document
	for _, doc := range kv.memtable {
		text := documentValueToString(doc.Value)
		if r.MatchString(text) {
			results = append(results, doc)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})
	return results
}

func (kv *KVStore) LoadFromJSONFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	var rawDocs []map[string]interface{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&rawDocs); err != nil {
		return err
	}
	for _, m := range rawDocs {
		var key string
		if id, ok := m["id"]; ok {
			key = fmt.Sprintf("%v", id)
		} else {
			key = fmt.Sprintf("%d", time.Now().UnixNano())
		}
		doc := Document{
			Key:       key,
			Value:     m,
			Timestamp: time.Now().UnixNano(),
		}
		kv.memtableLock.Lock()
		kv.memtable[doc.Key] = doc
		kv.memtableLock.Unlock()
		kv.bloom.Add(doc.Key)
		kv.indexDocument(doc)
	}
	return nil
}

type Filter struct {
	Field    string
	Operator string
	Value    string
}

func main() {
	gob.Register(map[string]any{})
	store, err := NewKVStore("data")
	if err != nil {
		fmt.Println("Error initializing KVStore:", err)
		return
	}
	defer store.Close()
	start := time.Now()
	err = store.LoadFromJSONFile("charge_master.json")
	if err != nil {
		fmt.Println("Error loading from JSON file:", err)
	} else {
		fmt.Println("JSON data loaded successfully.")
	}
	fmt.Println("Time to load JSON data:", time.Since(start))
	term := "33965F"
	termResults := store.TermQuery(term)
	fmt.Printf("\nTermQuery Results (term: \"%s\"):\n", term)
	for _, d := range termResults {
		fmt.Printf("Key: %s, Value: %s\n", d.Key, documentValueToString(d.Value))
	}
	filters := []Filter{
		{Field: "work_item_id", Operator: "eq", Value: "324"},
	}
	start = time.Now()
	advancedResults := store.Search("33965F", filters...)
	fmt.Printf("\nAdvancedQuery Results (keyword: \"33965F\", filter: field1 eq 'val'):\n")
	for _, d := range advancedResults {
		fmt.Printf("Key: %s, Value: %s\n", d.Key, documentValueToString(d.Value))
	}
	fmt.Println("Time to perform AdvancedQuery:", time.Since(start))
}
