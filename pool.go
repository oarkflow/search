package search

import (
	"github.com/oarkflow/search/lib"
)

var (
	tokensPool = lib.NewPool[map[string]int](func() map[string]int { return make(map[string]int) })
	indexPool  = lib.NewPool[*IndexParams](func() *IndexParams { return &IndexParams{} })
)
