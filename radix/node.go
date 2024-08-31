package radix

import (
	"maps"

	"github.com/oarkflow/search/lib"
)

type node struct {
	Subword  []rune
	Children map[rune]*node
	Infos    map[int64]float64
}

func newNode(subword []rune) *node {
	n := nodePool.Get()
	n.Subword = subword
	// Clear the maps (or reinitialize if needed)
	for k := range n.Children {
		delete(n.Children, k)
	}
	for k := range n.Infos {
		delete(n.Infos, k)
	}
	return n
}

func (n *node) putNode() {
	nodePool.Put(n)
}

func (n *node) addData(id int64, frequency float64) {
	n.Infos[id] = frequency
}

func (n *node) addChild(child *node) {
	if len(child.Subword) > 0 {
		n.Children[child.Subword[0]] = child
	}
}

func (n *node) removeChild(child *node) {
	if len(child.Subword) > 0 {
		delete(n.Children, child.Subword[0])
	}
}

func (n *node) removeData(id int64) {
	delete(n.Infos, id)
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
				maps.Copy(results, currNode.Infos)
			}
		} else {
			maps.Copy(results, currNode.Infos)
		}

		for _, child := range currNode.Children {
			stack = append(stack, [2]interface{}{child, append(currWord, child.Subword...)})
		}
	}
	n.putNode()
	return results
}

func mergeNodes(a *node, b *node) {
	a.Subword = append(a.Subword, b.Subword...)
	a.Infos = b.Infos
	a.Children = b.Children
}
