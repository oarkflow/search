package main

//
//import (
//	"bufio"
//	"bytes"
//	"encoding/binary"
//	"encoding/gob"
//	"fmt"
//	"io"
//	"log"
//	"os"
//	"regexp"
//	"strings"
//	"sync"
//	"syscall"
//	"time"
//
//	"github.com/klauspost/compress/zstd"
//	"github.com/oarkflow/json"
//	"github.com/oarkflow/json/jsonmap"
//)
//
//const (
//	defaultBlockSize = 1 << 20 // 1 MiB raw
//	blockMagic       = "BLK0"
//	blockVersion     = uint16(1)
//)
//
//// RecordPos points to one JSON line inside a compressed block.
//type RecordPos struct {
//	Block     int // which block
//	LineInBlk int // which line inside that block
//}
//
//// blockHeader is stored uncompressed before each zstd frame.
//type blockHeader struct {
//	Magic    [4]byte // "BLK0"
//	Version  uint16
//	CompLen  uint32 // length of compressed payload
//	PlainLen uint32 // length before compression
//}
//
//// JSONLineStore manages compressed blocks + in-RAM index.
//type JSONLineStore struct {
//	basePath  string  // original JSON (array or ndjson)
//	blkPath   string  // basePath + ".blk"
//	idxPath   string  // basePath + ".idx"
//	blockSize int     // raw bytes threshold per block
//	blockOffs []int64 // file offsets of each block header
//	index     map[string][]RecordPos
//	cache     map[int][][]byte // decompressed block → lines
//	mu        sync.RWMutex
//}
//
//// New returns a store for the given JSON path.
//func New(path string) *JSONLineStore {
//	return &JSONLineStore{
//		basePath:  path,
//		blkPath:   path + ".blk",
//		idxPath:   path + ".idx",
//		blockSize: defaultBlockSize,
//		index:     make(map[string][]RecordPos),
//		cache:     make(map[int][][]byte),
//	}
//}
//
//func main() {
//	start := time.Now()
//	store := New("charge_master.json")
//	if err := store.Open(); err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println("Indexing took", time.Since(start))
//
//	start = time.Now()
//	results, err := store.Search("G0365")
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println("Search took", time.Since(start))
//	for _, line := range results {
//		fmt.Println(string(line))
//	}
//}
//
//// Open prepares .blk and .idx (building or loading as needed).
//func (s *JSONLineStore) Open() error {
//	// 1) build blocks if missing
//	if !fileExists(s.blkPath) {
//		ndPath, err := dumpNDJSONIfNeeded(s.basePath)
//		if err != nil {
//			return err
//		}
//		if err := s.buildBlocks(ndPath); err != nil {
//			return err
//		}
//		// clean up temporary ndjson
//		if ndPath != s.basePath {
//			os.Remove(ndPath)
//		}
//	}
//	// 2) load or build index
//	if err := s.loadIndex(); err == nil {
//		return nil
//	}
//	return s.buildIndex()
//}
//
//// fileExists returns whether the given file path exists.
//func fileExists(path string) bool {
//	_, err := os.Stat(path)
//	return err == nil
//}
//
//// dumpNDJSONIfNeeded converts a JSON array to ndjson in a temp file, if needed.
//// Returns the path to an ndjson file (either original or newly created).
//func dumpNDJSONIfNeeded(inPath string) (string, error) {
//	f, err := os.Open(inPath)
//	if err != nil {
//		return "", err
//	}
//	defer f.Close()
//
//	r := bufio.NewReader(f)
//	first, err := r.Peek(1)
//	if err != nil {
//		return "", err
//	}
//	if first[0] != '[' {
//		return inPath, nil
//	}
//
//	all, err := io.ReadAll(r)
//	if err != nil {
//		return "", err
//	}
//	var arr []json.RawMessage
//	if err := jsonmap.Unmarshal(all, &arr); err != nil {
//		return "", err
//	}
//
//	outPath := inPath + ".ndjson"
//	of, err := os.Create(outPath)
//	if err != nil {
//		return "", err
//	}
//	defer of.Close()
//	for _, raw := range arr {
//		of.Write(raw)
//		of.Write([]byte("\n"))
//	}
//	return outPath, nil
//}
//
//// buildBlocks reads inputPath as ndjson, splits into raw blocks, compresses each,
//// and writes header+zstd payload sequentially to s.blkPath.
//func (s *JSONLineStore) buildBlocks(inputPath string) error {
//	in, err := os.Open(inputPath)
//	if err != nil {
//		return err
//	}
//	defer in.Close()
//
//	out, err := os.Create(s.blkPath)
//	if err != nil {
//		return err
//	}
//	defer out.Close()
//
//	scanner := bufio.NewScanner(in)
//	// if lines might exceed 64K, increase buffer:
//	// scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
//
//	var buf bytes.Buffer
//	enc, _ := zstd.NewWriter(nil)
//	encode := enc.EncodeAll
//
//	writeBlock := func() error {
//		if buf.Len() == 0 {
//			return nil
//		}
//		raw := buf.Bytes()
//		comp := encode(raw, nil)
//
//		hdrSize := binary.Size(blockHeader{})
//		tmp := make([]byte, hdrSize)
//		copy(tmp[0:], []byte(blockMagic))
//		binary.BigEndian.PutUint16(tmp[4:], blockVersion)
//		binary.BigEndian.PutUint32(tmp[6:], uint32(len(comp)))
//		binary.BigEndian.PutUint32(tmp[10:], uint32(len(raw)))
//
//		if _, err := out.Write(tmp); err != nil {
//			return err
//		}
//		if _, err := out.Write(comp); err != nil {
//			return err
//		}
//
//		buf.Reset()
//		return nil
//	}
//
//	for scanner.Scan() {
//		line := scanner.Bytes()
//		buf.Write(line)
//		buf.WriteByte('\n')
//		if buf.Len() >= s.blockSize {
//			if err := writeBlock(); err != nil {
//				return err
//			}
//		}
//	}
//	if err := scanner.Err(); err != nil {
//		return err
//	}
//	return writeBlock()
//}
//
//// buildIndex memory-maps the .blk file, scans headers, decompresses each block in
//// parallel, tokenizes lines, and builds the in-RAM inverted index.
//func (s *JSONLineStore) buildIndex() error {
//	f, err := os.Open(s.blkPath)
//	if err != nil {
//		return err
//	}
//	defer f.Close()
//	fi, _ := f.Stat()
//
//	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
//	if err != nil {
//		return err
//	}
//	defer syscall.Munmap(data)
//
//	var offs []int64
//	pos := int64(0)
//	hdrSz := int64(binary.Size(blockHeader{}))
//	for pos < int64(len(data)) {
//		if string(data[pos:pos+4]) != blockMagic {
//			return fmt.Errorf("bad magic at %d", pos)
//		}
//		if v := binary.BigEndian.Uint16(data[pos+4:]); v != blockVersion {
//			return fmt.Errorf("bad version %d at %d", v, pos)
//		}
//		cSz := int64(binary.BigEndian.Uint32(data[pos+6:]))
//		offs = append(offs, pos)
//		pos += hdrSz + cSz
//	}
//	s.blockOffs = offs
//	s.index = make(map[string][]RecordPos)
//
//	dec, _ := zstd.NewReader(nil)
//	defer dec.Close()
//
//	var wg sync.WaitGroup
//	ch := make(chan struct {
//		blkID int
//		raw   []byte
//	}, len(offs))
//
//	for blkID, off := range offs {
//		wg.Add(1)
//		go func(blkID int, off int64) {
//			defer wg.Done()
//			cSz := int(binary.BigEndian.Uint32(data[off+6:]))
//			ct := data[off+hdrSz : off+hdrSz+int64(cSz)]
//			raw, _ := dec.DecodeAll(ct, nil)
//			ch <- struct {
//				blkID int
//				raw   []byte
//			}{blkID, raw}
//		}(blkID, off)
//	}
//	go func() {
//		wg.Wait()
//		close(ch)
//	}()
//
//	wordRE := regexp.MustCompile(`[_A-Za-z0-9]+`)
//	for rec := range ch {
//		lines := bytesSplit(rec.raw, '\n')
//		for i, ln := range lines {
//			for _, w := range wordRE.FindAll(ln, -1) {
//				key := strings.ToLower(string(w))
//				s.index[key] = append(s.index[key], RecordPos{Block: rec.blkID, LineInBlk: i})
//			}
//		}
//	}
//	return s.saveIndex()
//}
//
//// saveIndex writes blockOffs + index to .idx via gob.
//func (s *JSONLineStore) saveIndex() error {
//	f, err := os.Create(s.idxPath)
//	if err != nil {
//		return err
//	}
//	defer f.Close()
//	// wrap file writer with zstd encoder for compression
//	zenc, err := zstd.NewWriter(f)
//	if err != nil {
//		return err
//	}
//	defer zenc.Close()
//	enc := gob.NewEncoder(zenc)
//	return enc.Encode(struct {
//		BlockOffs []int64
//		Index     map[string][]RecordPos
//	}{s.blockOffs, s.index})
//}
//
//// loadIndex reads the .idx file into memory.
//func (s *JSONLineStore) loadIndex() error {
//	f, err := os.Open(s.idxPath)
//	if err != nil {
//		return err
//	}
//	defer f.Close()
//	// wrap file reader with zstd decoder for decompression
//	zdec, err := zstd.NewReader(f)
//	if err != nil {
//		return err
//	}
//	defer zdec.Close()
//	var data struct {
//		BlockOffs []int64
//		Index     map[string][]RecordPos
//	}
//	if err := gob.NewDecoder(zdec).Decode(&data); err != nil {
//		return err
//	}
//	s.blockOffs = data.BlockOffs
//	s.index = data.Index
//	return nil
//}
//
//// Search returns all JSON lines containing term (case-insensitive).
//func (s *JSONLineStore) Search(term string) ([][]byte, error) {
//	s.mu.RLock()
//	defer s.mu.RUnlock()
//
//	key := strings.ToLower(term)
//	poses, ok := s.index[key]
//	if !ok {
//		return nil, nil
//	}
//
//	// mmap + decompressor
//	f, err := os.Open(s.blkPath)
//	if err != nil {
//		return nil, err
//	}
//	defer f.Close()
//	fi, _ := f.Stat()
//	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
//	if err != nil {
//		return nil, err
//	}
//	defer syscall.Munmap(data)
//	dec, _ := zstd.NewReader(nil)
//	defer dec.Close()
//
//	hdrSz := int64(binary.Size(blockHeader{}))
//	var out [][]byte
//	for _, rp := range poses {
//		lines, cached := s.cache[rp.Block]
//		if !cached {
//			off := s.blockOffs[rp.Block]
//			cSz := int(binary.BigEndian.Uint32(data[off+6:]))
//			ct := data[off+hdrSz : off+hdrSz+int64(cSz)]
//			raw, _ := dec.DecodeAll(ct, nil)
//			lines = bytesSplit(raw, '\n')
//			s.cache[rp.Block] = lines
//		}
//		if rp.LineInBlk < len(lines) {
//			out = append(out, lines[rp.LineInBlk])
//		}
//	}
//	return out, nil
//}
//
//// bytesSplit splits b on sep (like bytes.Split but simpler).
//func bytesSplit(b []byte, sep byte) [][]byte {
//	var res [][]byte
//	start := 0
//	for i, c := range b {
//		if c == sep {
//			res = append(res, b[start:i])
//			start = i + 1
//		}
//	}
//	if start < len(b) {
//		res = append(res, b[start:])
//	}
//	return res
//}
