package main

import (
	"fmt"
	"log"
	"time"

	v1 "github.com/oarkflow/search/v1"
)

func main() {
	jsonFilePath := "charge_master.json"
	startTime := time.Now()
	index, err := v1.BuildIndex(jsonFilePath)
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	fmt.Printf("Index built for %d documents in %s\n", index.TotalDocs, time.Since(startTime))
	termQ := v1.NewTermQuery("33965", true, 1)
	rangeQ := v1.NewRangeQuery("work_item_id", 30, 40)
	boolQ := v1.BoolQuery{
		Must: []v1.Query{termQ, rangeQ},
	}
	scoredDocs := v1.ScoreQuery(boolQ, index, "33965")
	page := 1
	perPage := 10
	paginatedResults := v1.Paginate(scoredDocs, page, perPage)
	fmt.Printf("Found %d matching documents (showing page %d):\n", len(scoredDocs), page)
	for _, sd := range paginatedResults {
		rec := index.Documents[sd.DocID]
		fmt.Printf("DocID: %d | Score: %.4f | Data: %+v\n", sd.DocID, sd.Score, rec)
	}
}
