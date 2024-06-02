package radix

import (
	"maps"

	"github.com/oarkflow/search/lib"
)

type node struct {
	subword  []rune
	children map[rune]*node
	infos    map[int64]float64
}

func newNode(subword []rune) *node {
	n := nodePool.Get()
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

func (n *node) putNode() {
	nodePool.Put(n)
}

func (n *node) addData(id int64, frequency float64) {
	n.infos[id] = frequency
}

func (n *node) addChild(child *node) {
	if len(child.subword) > 0 {
		n.children[child.subword[0]] = child
	}
}

func (n *node) removeChild(child *node) {
	if len(child.subword) > 0 {
		delete(n.children, child.subword[0])
	}
}

func (n *node) removeData(id int64) {
	delete(n.infos, id)
}

func (n *node) findData(word []rune, term []rune, tolerance int, exact bool) map[int64]float64 {
	results := make(map[int64]float64)
	stack := [][2]interface{}{{n, word}}

	for len(stack) > 0 {
		currNode, currWord := stack[len(stack)-1][0].(*node), stack[len(stack)-1][1].([]rune)
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
	n.putNode()
	return results
}

func mergeNodes(a *node, b *node) {
	a.subword = append(a.subword, b.subword...)
	a.infos = b.infos
	a.children = b.children
}
