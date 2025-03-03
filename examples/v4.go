package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goccy/go-json"

	"github.com/oarkflow/search/lib"
)

// Record represents the JSON record structure.
type Record struct {
	ChargeType         string  `json:"charge_type"`
	ClientInternalCode string  `json:"client_internal_code"`
	ClientProcDesc     string  `json:"client_proc_desc"`
	CpthcpcsCode       string  `json:"cpt_hcpcs_code"`
	EffectiveDate      string  `json:"effective_date"`
	EndEffectiveDate   *string `json:"end_effective_date"`
	PatientStatusID    *int    `json:"patient_status_id"`
	WorkItemID         int     `json:"work_item_id"`
}

// AggregationResult is used in the demo main function.
type AggregationResult struct {
	Count int
	Sum   int
}

// StreamResult holds a record and the results from all callbacks.
type StreamResult struct {
	Record  Record
	Results []any
}

// RecordCallback defines the callback signature.
type RecordCallback func(rec Record) (any, error)

// StreamJSONRecords opens the JSON file and streams each record through all callbacks.
// It returns a channel that yields a StreamResult for each record.
func StreamJSONRecords(filePath string, callbacks ...RecordCallback) (<-chan StreamResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	// Use a buffered reader for efficient I/O.
	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)

	// Expect the JSON file to be an array (i.e. it starts with '[').
	token, err := decoder.Token()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		file.Close()
		return nil, fmt.Errorf("expected '[' at beginning of JSON array, got: %v", token)
	}

	outChan := make(chan StreamResult, 100) // buffered channel

	// Start a goroutine to decode and process records.
	go func() {
		// Ensure the file is closed and the channel is closed when done.
		defer file.Close()
		defer close(outChan)
		// Loop until there are no more records.
		for decoder.More() {
			var rec Record
			if err := decoder.Decode(&rec); err != nil {
				log.Printf("Error decoding record: %v", err)
				continue // Skip malformed records.
			}
			// Process the record through each callback.
			var cbResults []any
			for _, callback := range callbacks {
				res, err := callback(rec)
				if err != nil {
					// Log the error and append a nil result.
					log.Printf("Callback error: %v", err)
					cbResults = append(cbResults, nil)
				} else {
					cbResults = append(cbResults, res)
				}
			}
			// Yield the result.
			outChan <- StreamResult{
				Record:  rec,
				Results: cbResults,
			}
		}
		// Optionally, check the closing token.
		if token, err := decoder.Token(); err != nil {
			log.Printf("Error reading closing token: %v", err)
		} else if delim, ok := token.(json.Delim); !ok || delim != ']' {
			log.Printf("Expected ']' at end of JSON array, got: %v", token)
		}
	}()

	return outChan, nil
}

func main() {
	filePath := "charge_master.json"

	// Define multiple callbacks to process each record.
	callbacks := []RecordCallback{
		// Callback 1: Double the WorkItemID.
		func(rec Record) (any, error) {
			return rec.WorkItemID * 2, nil
		},
		// Callback 2: Return the length of the ClientProcDesc string.
		func(rec Record) (any, error) {
			return len(rec.ClientProcDesc), nil
		},
		// Callback 3: Check if EffectiveDate is non-empty.
		func(rec Record) (any, error) {
			return rec.EffectiveDate != "", nil
		},
	}

	start := time.Now()
	beforeMem := lib.Stats()

	// Get the generator channel.
	stream, err := StreamJSONRecords(filePath, callbacks...)
	if err != nil {
		log.Fatalf("Error starting stream: %v", err)
	}

	// Process streaming results. Here we simply aggregate the WorkItemIDs.
	aggregated := AggregationResult{}
	recordCount := 0
	for res := range stream {
		recordCount++
		aggregated.Count++
		aggregated.Sum += res.Record.WorkItemID
		// You can also process the per‑record callback results:
		// fmt.Printf("Record: %+v, Callback Results: %+v\n", res.Record, res.Results)
	}

	afterMem := lib.Stats()
	elapsed := time.Since(start)
	fmt.Printf("Processed %d records\n", recordCount)
	fmt.Printf("Aggregated Result: Count = %d, Sum = %d\n", aggregated.Count, aggregated.Sum)
	fmt.Printf("Memory Usage: Before: %dMB, After: %dMB, Delta: %dMB\n", beforeMem, afterMem, afterMem-beforeMem)
	fmt.Printf("Latency: %s\n", elapsed)
}
