package radix

import (
	"github.com/oarkflow/search/lib"
)

var (
	nodePool lib.Pool[*Node]
)

func init() {
	nodePool = lib.NewPool[*Node](func() *Node {
		return &Node{
			Children: make(map[rune]*Node),
			Infos:    make(map[int64]float64),
		}
	})
}
