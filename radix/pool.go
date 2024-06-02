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
			children: make(map[rune]*node),
			infos:    make(map[int64]float64),
		}
	})
}
