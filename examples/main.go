package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/tokenizer"
	"github.com/oarkflow/search/web"
)

func main() {
	// web.StartServer("0.0.0.0:8001")
	// httpTest()
	testMap()
	// testStruct()
	// testString()
}

func httpTest() {
	db, _ := search.New[map[string]any](&search.Config{
		DefaultLanguage: tokenizer.ENGLISH,
		TokenizerConfig: &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		},
		Key:           "icd",
		ResetPath:     true,
		IndexKeys:     []string{"charge_type", "client_internal_code", "client_proc_desc", "cpt_hcpcs_code", "work_item_id"},
		FieldsToStore: []string{"charge_type", "client_internal_code", "client_proc_desc", "cpt_hcpcs_code", "work_item_id"},
	})
	data := readFileAsMap("cpt_codes.json")
	start := time.Now()
	for _, d := range data {
		_, errs := db.Insert(d)
		if errs != nil {
			panic(errs)
		}
	}
	search.AddEngine("icd", db)
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(start))
	web.StartServer("0.0.0.0:8001")
}

type ICD struct {
	Code string `json:"code" index:"code"`
	Desc string `json:"desc" index:"desc"`
}

func readData() (icds []ICD) {
	jsonData, err := os.ReadFile("icd10_codes.json")
	if err != nil {
		fmt.Printf("failed to read json file, error: %v", err)
		return
	}

	if err := json.Unmarshal(jsonData, &icds); err != nil {
		fmt.Printf("failed to unmarshal json file, error: %v", err)
		return
	}
	return
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

func readFromString() []string {
	return []string{
		"Salmonella pneumonia",
		"Diabetes uncontrolled",
	}
}

func readFromInt() []int {
	return []int{
		10,
		100,
		20,
	}
}

func memoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / 1024 / 1024
}

func testMap() {
	icds := readFileAsMap("icd10_codes.json")
	db, _ := search.New[map[string]any](&search.Config{
		DefaultLanguage: tokenizer.ENGLISH,
		TokenizerConfig: &tokenizer.Config{
			EnableStemming:  true,
			EnableStopWords: true,
		},
	})
	var startTime = time.Now()
	for _, icd := range icds {
		db.Insert(icd)
	}
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(startTime))
	startTime = time.Now()
	s, err := db.Search(&search.Params{
		Query: "QUANTITATIVE",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime))
	fmt.Println(s.Hits)
}

func testStruct() {
	data := readData()
	ftsSearch, _ := search.New[ICD](&search.Config{
		TokenizerConfig: &tokenizer.Config{
			EnableStopWords: true,
			EnableStemming:  true,
		},
	})
	errs := ftsSearch.InsertBatch(data, 1000)
	if len(errs) > 0 {
		for _, err := range errs {
			panic(err)
		}
	}
	err := ftsSearch.Compress()
	if err != nil {
		panic(err)
	}
	start := time.Now()
	s, err := ftsSearch.Search(&search.Params{
		Extra: map[string]any{
			"code": "Z9981",
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(s.Hits)
	fmt.Printf("Time to search %s", time.Since(start))
}

func testString() {
	data := readFromInt()
	ftsSearch, _ := search.New[int](&search.Config{
		TokenizerConfig: &tokenizer.Config{
			EnableStopWords: true,
			EnableStemming:  true,
		},
	})
	errs := ftsSearch.InsertBatch(data, 1000)
	if len(errs) > 0 {
		for _, err := range errs {
			panic(err)
		}
	}
	start := time.Now()
	s, err := ftsSearch.Search(&search.Params{
		Query: "10",
		Exact: true,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(s.Hits)
	fmt.Printf("Time to search %s", time.Since(start))
}
