package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/oarkflow/search/lib"
)

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

type AggregationResult struct {
	Count int
	Sum   int
}

func processRecord(rec Record) (int, int) {
	return 1, rec.WorkItemID
}

func worker(records <-chan Record, resultChan chan<- AggregationResult, wg *sync.WaitGroup) {
	defer wg.Done()
	localResult := AggregationResult{}
	for rec := range records {
		count, sum := processRecord(rec)
		localResult.Count += count
		localResult.Sum += sum
	}
	resultChan <- localResult
}

func main() {
	filePath := "charge_master.json"
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer func() {
		_ = file.Close()
	}()
	reader := bufio.NewReader(file)
	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		log.Fatalf("Failed to read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		log.Fatalf("Expected '[' but got %v", token)
	}
	numWorkers := runtime.NumCPU()
	recordChan := make(chan Record, 1000)
	resultChan := make(chan AggregationResult, numWorkers)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(recordChan, resultChan, &wg)
	}
	start := time.Now()
	beforeMem := lib.Stats()
	for decoder.More() {
		var rec Record
		if err := decoder.Decode(&rec); err != nil {
			log.Printf("Skipping invalid record: %v", err)
			continue
		}
		recordChan <- rec
	}
	close(recordChan)
	wg.Wait()
	close(resultChan)
	finalResult := AggregationResult{}
	for res := range resultChan {
		finalResult.Count += res.Count
	}
	token, err = decoder.Token()
	if err != nil {
		log.Fatalf("Failed to read closing token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != ']' {
		log.Fatalf("Expected ']' but got %v", token)
	}
	afterMem := lib.Stats()
	fmt.Printf("Usage: Before: %dMB, After: %dMB, Delta: %dMB\n", beforeMem, afterMem, afterMem-beforeMem)
	fmt.Printf("Aggregated Result: Count = %d, Latency: %s\n", finalResult.Count, time.Since(start))
}
