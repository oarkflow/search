package lib

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"runtime"
	"unsafe"
)

func ToString(value interface{}) string {
	return fmt.Sprint(value)
}

func FromByte(b []byte) string {
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

func ReadFileAsMap(file string) (icds []map[string]any) {
	jsonData, err := os.ReadFile(file)
	if err != nil {
		panic("failed to read json file, error: " + err.Error())
		return
	}

	if err := json.Unmarshal(jsonData, &icds); err != nil {
		fmt.Printf("failed to unmarshal json file, error: %v", err)
		return
	}
	return
}

func Stats() uint64 {
	return GetMemoryUsage() / (1024 * 1024)
}

// GetMemoryUsage returns the current memory usage in bytes.
func GetMemoryUsage() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Alloc
}
func CRC32Checksum(data interface{}) int64 {
	bt, err := json.Marshal(data)
	if err != nil {
		return 0
	}
	table := crc32.MakeTable(crc32.IEEE)
	checksum := crc32.Checksum(bt, table)
	return int64(checksum)
}
