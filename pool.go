package search

import (
	"github.com/oarkflow/search/lib"
)

var (
	tokensPool lib.Pool[map[string]int]
)

func init() {
	tokensPool = lib.NewPool[map[string]int](func() map[string]int { return make(map[string]int) })
}
