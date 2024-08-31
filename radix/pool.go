package radix

import (
	"github.com/oarkflow/search/lib"
)

var (
	nodePool lib.Pool[*node]
)

func init() {
	nodePool = lib.NewPool[*node](func() *node {
		return &node{
			Children: make(map[rune]*node),
			Infos:    make(map[int64]float64),
		}
	})
}
