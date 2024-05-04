package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/tokenizer"
)

func TestMap(t *testing.T) {
	icds := readFileAsMap("icd10_codes.json")
	db, _ := search.New[map[string]any](&search.Config{
		DefaultLanguage: tokenizer.ENGLISH,
		TokenizerConfig: &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		},
	})
	var startTime = time.Now()
	db.InsertBatch(icds, runtime.NumCPU())
	/*for _, icd := range icds {
		db.Insert(icd)
	}*/
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(startTime))
	startTime = time.Now()
	s, err := db.Search(&search.Params{
		Query: "presence",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime))
	fmt.Println(len(s.Hits))
}

func readFileAsMap(file string) (icds []map[string]any) {
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
