package lib

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"runtime"
	"unsafe"

	"github.com/oarkflow/msgpack"
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
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / (1024 * 1024)
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

// Encode and decode functions to handle type serialization.
func Encode[V any](value V) []byte {
	jsonData, err := msgpack.Marshal(value)
	if err != nil {
		return nil
	}
	return jsonData
}

func Decode[V any](data []byte) V {
	var value V
	err := msgpack.Unmarshal(data, &value)
	if err != nil {
		panic(err)
		return *new(V)
	}
	return value
}
