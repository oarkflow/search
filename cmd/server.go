package main

import (
	_ "embed"
	"flag"
	"fmt"

	"github.com/oarkflow/search/web"
)

var (
	hostPtr = flag.String("host", "0.0.0.0", "Domain name or IP")
	portPtr = flag.String("port", "3000", "Port available to be used on server")
)

func main() {
	flag.Parse()
	addr := fmt.Sprintf("%s:%s", *hostPtr, *portPtr)
	web.StartServer(addr)
}
