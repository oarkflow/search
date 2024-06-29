package lib

import (
	"encoding/json"
	"fmt"
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
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / (1024 * 1024)
}
