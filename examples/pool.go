package main

import (
	"bufio"
	"fmt"
	"os"
)

// Assuming you have a struct representing your data
type Record struct {
	// Define your fields here
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Email string `json:"email"`
	// Add more fields if necessary
}

// Function to read records from file and chunk them
func ReadRecords[T any](filename string, chunkSize int) ([][]T, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records [][]T
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // Set buffer to 1MB
	for scanner.Scan() {
		line := scanner.Text()
		// Assuming each line contains a JSON record
		// You can parse each line and decode it into a Record struct
		var record T
		// Implement your own logic to parse the line into the Record struct
		// Example: json.Unmarshal([]byte(line), &record)
		// Here, I'm just printing the line for demonstration
		fmt.Println(line)
		// Append the record to the current chunk
		currentChunk := len(records) - 1
		if currentChunk < 0 || len(records[currentChunk]) == chunkSize {
			records = append(records, []T{})
			currentChunk++
		}
		records[currentChunk] = append(records[currentChunk], record)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func main() {
	// Example usage
	filename := "icd10_codes.json"
	chunkSize := 10
	chunks, err := ReadRecords[Record](filename, chunkSize)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Number of chunks:", len(chunks))
	for i, chunk := range chunks {
		fmt.Printf("Chunk %d:\n", i+1)
		for _, record := range chunk {
			// Process each record in the chunk
			fmt.Printf("Name: %s, Age: %d, Email: %s\n", record.Name, record.Age, record.Email)
		}
	}
}
