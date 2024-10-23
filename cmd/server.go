package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/web"
)

var (
	hostPtr       = flag.String("host", "0.0.0.0", "Domain name or IP")
	portPtr       = flag.String("port", "3000", "Port available to be used on server")
	filePtr       = flag.String("file", "", "Index file available to be used on server")
	groupFilesPtr = flag.String("group-files", "", "Index files available to be used on server")
	indexKeyPtr   = flag.String("key", "", "Index index key available to be used on server")
	sortFieldPtr  = flag.String("sort-field", "", "Index sort field used to sort")
)

type File struct {
	Path      string `json:"path"`
	Key       string `json:"key"`
	SortField string `json:"sort_field"`
}

func main() {
	flag.Parse()
	addr := fmt.Sprintf("%s:%s", *hostPtr, *portPtr)
	if *filePtr != "" && *indexKeyPtr != "" {
		go func(path, key, sortField string) {
			err := indexFile(path, key, sortField)
			if err != nil {
				panic(err)
			}
		}(*filePtr, *indexKeyPtr, *sortFieldPtr)
	}
	if *groupFilesPtr != "" {
		var files []File
		data, err := os.ReadFile(*groupFilesPtr)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(data, &files)
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			go func(path, key, sortField string) {
				err := indexFile(path, key, sortField)
				if err != nil {
					panic(err)
				}
			}(file.Path, file.Key, file.SortField)
		}
	}
	web.StartServer(addr)
}

func indexFile(path, key, sortField string) error {
	data := lib.ReadFileAsMap(path)
	engine, err := search.GetOrSetEngine[map[string]any](key, &search.Config{
		Storage:          "mmap",
		Key:              key,
		CleanupPeriod:    2 * time.Minute,
		EvictionDuration: 3 * time.Minute,
		ResetPath:        true,
		OffloadIndex:     true,
		SortField:        sortField,
	})
	if err != nil {
		return err
	}
	engine.InsertWithPool(data, runtime.NumCPU(), 1000)
	fmt.Println("Indexed", key)
	return nil
}
