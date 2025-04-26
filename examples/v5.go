package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/xid"
)

type Document struct {
	Key   string
	Value any
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

	invertedIndex map[string]map[string][]int
	indexLock     sync.RWMutex
}

func NewKVStore(dir string) (*KVStore, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}
	id := xid.New().String()
	walPath := filepath.Join(dir, id+"_wal.log")
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, id+"_log.dat")
	valLog, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	store := &KVStore{
		dir:            dir,
		memtable:       make(map[string]Document),
		walFile:        walFile,
		valueLogFile:   valLog,
		stopCompaction: make(chan struct{}),
		bloom:          NewBloomFilter(1 << 16),
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
	return enc.Encode(doc)
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

func (kv *KVStore) AddDocument(doc Document) error {
	if err := kv.writeWAL(doc); err != nil {
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
	return nil
}

func (kv *KVStore) AddKeyVal(key string, value any) error {
	doc := Document{Key: key, Value: value}
	return kv.AddDocument(doc)
}

func (kv *KVStore) UpdateDocument(key string, value any) error {
	kv.memtableLock.RLock()
	oldDoc, exists := kv.memtable[key]
	kv.memtableLock.RUnlock()
	if exists {
		kv.removeFromIndex(oldDoc)
	}
	return kv.AddKeyVal(key, value)
}

func (kv *KVStore) DeleteDocument(key string) error {
	doc := Document{
		Key:   key,
		Value: nil,
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

func (kv *KVStore) AddDocuments(docs ...Document) error {
	for _, doc := range docs {
		err := kv.AddDocument(doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (kv *KVStore) Get(term string) []Document {
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

func toString(val any) string {
	switch val := val.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int32, int64, int8, int16, uint, uint32, uint64, uint8, uint16:
		return fmt.Sprintf("%d", val)
	case float32:
		buf := make([]byte, 0, 32)
		buf = strconv.AppendFloat(buf, float64(val), 'f', -1, 64)
		return string(buf)
	case float64:
		buf := make([]byte, 0, 32)
		buf = strconv.AppendFloat(buf, val, 'f', -1, 64)
		return string(buf)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (kv *KVStore) LoadFromJSONFile(filename, keyField string) error {
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
		if id, ok := m[keyField]; ok {
			key = toString(id)
		} else {
			key = xid.New().String()
		}
		err := kv.AddKeyVal(key, m)
		if err != nil {
			return err
		}
	}
	return nil
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
	err = store.LoadFromJSONFile("charge_master.json", "id")
	if err != nil {
		fmt.Println("Error loading from JSON file:", err)
	} else {
		fmt.Println("JSON data loaded successfully.")
	}
	fmt.Println("Time to load JSON data:", time.Since(start))
	term := "31686207"
	start = time.Now()
	termResults := store.Get(term)
	fmt.Println("Time to perform Get:", time.Since(start))
	fmt.Printf("\nGet Results (term: \"%s\"):\n", term)
	for _, d := range termResults {
		fmt.Printf("Key: %s, Value: %s\n", d.Key, documentValueToString(d.Value))
	}
}
