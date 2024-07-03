package radix

import (
	"maps"

	"github.com/oarkflow/search/hash"
	"github.com/oarkflow/search/lib"
)

type node[T hash.Hashable] struct {
	subword  []rune
	children map[rune]*node[T]
	infos    map[T]float64
}

func newNode[T hash.Hashable](subword []rune) *node[T] {
	n := &node[T]{
		infos:    make(map[T]float64),
		children: make(map[rune]*node[T]),
	}
	n.subword = subword
	// Clear the maps (or reinitialize if needed)
	for k := range n.children {
		delete(n.children, k)
	}
	for k := range n.infos {
		delete(n.infos, k)
	}
	return n
}

func (n *node[T]) addData(id T, frequency float64) {
	n.infos[id] = frequency
}

func (n *node[T]) addChild(child *node[T]) {
	if len(child.subword) > 0 {
		n.children[child.subword[0]] = child
	}
}

func (n *node[T]) removeChild(child *node[T]) {
	if len(child.subword) > 0 {
		delete(n.children, child.subword[0])
	}
}

func (n *node[T]) removeData(id T) {
	delete(n.infos, id)
}

func (n *node[T]) findData(word []rune, term []rune, tolerance int, exact bool) map[T]float64 {
	results := make(map[T]float64)
	stack := [][2]interface{}{{n, word}}

	for len(stack) > 0 {
		currNode, currWord := stack[len(stack)-1][0].(*node[T]), stack[len(stack)-1][1].([]rune)
		stack = stack[:len(stack)-1]

		if _, eq := lib.CommonPrefix(currWord, term); !eq && exact {
			break
		}

		if tolerance > 0 {
			if _, isBounded := lib.BoundedLevenshtein(currWord, term, tolerance); isBounded {
				maps.Copy(results, currNode.infos)
			}
		} else {
			maps.Copy(results, currNode.infos)
		}

		for _, child := range currNode.children {
			stack = append(stack, [2]interface{}{child, append(currWord, child.subword...)})
		}
	}
	return results
}

func mergeNodes[T hash.Hashable](a *node[T], b *node[T]) {
	a.subword = append(a.subword, b.subword...)
	a.infos = b.infos
	a.children = b.children
}
