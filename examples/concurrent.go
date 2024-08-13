package main

/*
import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type IndexEntry struct {
	Key    string
	Offset int64
}

// Sequentially scan the file and identify the start of each JSON object
func indexJSONFile(filePath, key string, indexChan chan<- IndexEntry) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)

	offset := int64(0)
	for {
		currentOffset := offset

		// Use json.RawMessage to manually decode the object after finding the start
		t, err := decoder.Token()
		if err != nil {
			break // EOF or error
		}

		// Check if the token is the start of a JSON object
		if delim, ok := t.(json.Delim); ok && delim.String() == "{" {
			objectOffset := currentOffset + decoder.InputOffset() - 1

			// Decode the object into a raw message (to avoid issues with direct decoding)
			var rawObj json.RawMessage
			if err := decoder.Decode(&rawObj); err == nil {
				// Unmarshal the raw message into a map to extract the key
				var obj map[string]interface{}
				if err := json.Unmarshal(rawObj, &obj); err == nil {
					if val, ok := obj[key]; ok {
						indexChan <- IndexEntry{Key: val.(string), Offset: objectOffset}
					}
				}
			} else {
				panic(err)
			}
		}

		// Update the current offset after processing
		offset = currentOffset + decoder.InputOffset()
	}

	return nil
}

func buildJSONIndex(filePath, key string) (map[string]int64, error) {
	index := make(map[string]int64)
	indexChan := make(chan IndexEntry, 100)
	var wg sync.WaitGroup

	start := time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := indexJSONFile(filePath, key, indexChan)
		if err != nil {
			fmt.Println("Error indexing file:", err)
		}
	}()

	go func() {
		wg.Wait()
		close(indexChan)
	}()

	for entry := range indexChan {
		index[entry.Key] = entry.Offset
	}

	elapsed := time.Since(start)
	fmt.Printf("Indexing took %s\n", elapsed)

	return index, nil
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

func fetchJSONObjects(filePath string, keys []string, index map[string]int64, resultChan chan<- map[string]interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for _, key := range keys {
		if offset, found := index[key]; found {
			obj, err := readJSONObject(filePath, offset)
			if err != nil {
				fmt.Println("Error reading JSON object:", err)
				continue
			}
			resultChan <- obj
		} else {
			fmt.Printf("Key %s not found in index\n", key)
		}
	}
}

func main() {
	filePath := "cpt_codes.json"
	key := "cpt_hcpcs_code" // Change this to your custom key

	// Build the index sequentially
	index, err := buildJSONIndex(filePath, key)
	if err != nil {
		fmt.Println("Error building index:", err)
		return
	}

	// Define specific keys to read
	keysToRead := []string{"90772", "90773"}

	// Fetch the JSON objects concurrently
	resultChan := make(chan map[string]interface{}, len(keysToRead))
	var wg sync.WaitGroup

	start := time.Now()

	// Splitting the work between workers
	numWorkers := 4
	chunkSize := (len(keysToRead) + numWorkers - 1) / numWorkers
	for i := 0; i < numWorkers; i++ {
		startIndex := i * chunkSize
		endIndex := startIndex + chunkSize
		if endIndex > len(keysToRead) {
			endIndex = len(keysToRead)
		}

		wg.Add(1)
		go fetchJSONObjects(filePath, keysToRead[startIndex:endIndex], index, resultChan, &wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for obj := range resultChan {
		jsonData, _ := json.MarshalIndent(obj, "", "  ")
		fmt.Println(string(jsonData))
	}

	elapsed := time.Since(start)
	fmt.Printf("Total fetch time: %s\n", elapsed)
}
*/
