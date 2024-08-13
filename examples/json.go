package main

/*
import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func buildJSONIndex(filePath string) ([]int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var offsets []int64
	reader := bufio.NewReader(file)

	var offset int64
	var inArray bool

	decoder := json.NewDecoder(reader)

	start := time.Now()

	for {
		t, err := decoder.Token()
		if err != nil {
			break // EOF or error
		}

		switch t := t.(type) {
		case json.Delim:
			if t.String() == "[" {
				inArray = true
			} else if t.String() == "{" && inArray {
				offset = decoder.InputOffset() - 1
				offsets = append(offsets, offset)
			}
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Indexing took %s\n", elapsed)

	return offsets, nil
}

func readJSONObject(filePath string, offset int64) (map[string]interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)

	var obj map[string]interface{}
	err = decoder.Decode(&obj)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func printMemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func main() {
	// Profiling CPU
	cpuProfile, err := os.Create("cpu_profile.prof")
	if err != nil {
		fmt.Println("Error creating CPU profile:", err)
		return
	}
	pprof.StartCPUProfile(cpuProfile)
	defer pprof.StopCPUProfile()

	// Start Memory Profiling
	memProfile, err := os.Create("mem_profile.prof")
	if err != nil {
		fmt.Println("Error creating memory profile:", err)
		return
	}
	defer pprof.WriteHeapProfile(memProfile)

	filePath := "cpt_codes.json"

	// Track start time for overall process
	start := time.Now()

	// Building the JSON index
	index, err := buildJSONIndex(filePath)
	if err != nil {
		fmt.Println("Error building index:", err)
		return
	}
	fmt.Println("Index built successfully", len(index))
	// Print memory usage after indexing
	printMemoryUsage()

	// Accessing the 10th object
	if len(index) > 9 {
		// Define specific keys to read
		keysToRead := []int{1, 4, 5, 9}

		// Track start time for reading specific objects
		for _, k := range keysToRead {
			offset := index[k]
			obj, err := readJSONObject(filePath, offset)
			if err != nil {
				fmt.Println("Error reading JSON object:", err)
				continue
			}

			jsonData, _ := json.MarshalIndent(obj, "", "  ")
			fmt.Printf("Object with key %d:\n%s\n", k, string(jsonData))
		}
	} else {
		fmt.Println("10th object does not exist.")
	}

	// Print final memory usage
	printMemoryUsage()

	// Track end time and calculate duration
	elapsed := time.Since(start)
	fmt.Printf("Total execution time: %s\n", elapsed)
}
*/
