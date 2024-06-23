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
	hostPtr = flag.String("host", "0.0.0.0", "Domain name or IP")
	portPtr = flag.String("port", "3000", "Port available to be used on server")
)

func main() {
	flag.Parse()
	addr := fmt.Sprintf("%s:%s", *hostPtr, *portPtr)
	icds := lib.ReadFileAsMap("/Users/sujit/Sites/oarkflow/search/examples/cpt_codes.json")
	engine, err := search.GetOrSetEngine[map[string]any]("cpt", &search.Config{
		Storage: "memory",
	})
	if err != nil {
		panic(err)
	}
	engine.InsertWithPool(icds, runtime.NumCPU(), 1000)
	web.StartServer(addr)
}
