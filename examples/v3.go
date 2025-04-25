package main

import (
	v1 "github.com/oarkflow/search/v1"
)

func main() {
	manager := v1.NewManager()
	manager.StartHTTP(":8080")
}
