package main

import (
	_ "embed"
	"flag"
	"fmt"
	"runtime"
	
	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
	"github.com/oarkflow/search/web"
)

var (
	hostPtr     = flag.String("host", "0.0.0.0", "Domain name or IP")
	portPtr     = flag.String("port", "3000", "Port available to be used on server")
	filePtr     = flag.String("file", "", "Index file available to be used on server")
	indexKeyPtr = flag.String("key", "", "Index index key available to be used on server")
)

func main() {
	flag.Parse()
	addr := fmt.Sprintf("%s:%s", *hostPtr, *portPtr)
	if *filePtr != "" && *indexKeyPtr != "" {
		icds := lib.ReadFileAsMap(*filePtr)
		engine, err := search.GetOrSetEngine[map[string]any](*indexKeyPtr, &search.Config{})
		if err != nil {
			panic(err)
		}
		engine.InsertWithPool(icds, runtime.NumCPU(), 1000)
		fmt.Println("Indexed", *indexKeyPtr)
	}
	web.StartServer(addr)
}
