package radix

import (
	"maps"

	"github.com/oarkflow/search/lib"
)

type Node struct {
	Subword  []rune            `json:"subword"`
	Children map[rune]*Node    `json:"children"`
	Infos    map[int64]float64 `json:"infos"`
}

func newNode(subword []rune) *Node {
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

func (n *Node) putNode() {
	nodePool.Put(n)
}

func (n *Node) addData(id int64, frequency float64) {
	n.Infos[id] = frequency
}

func (n *Node) addChild(child *Node) {
	if len(child.Subword) > 0 {
		n.Children[child.Subword[0]] = child
	}
}

func (n *Node) removeChild(child *Node) {
	if len(child.Subword) > 0 {
		delete(n.Children, child.Subword[0])
	}
}

func (n *Node) removeData(id int64) {
	delete(n.Infos, id)
}

func (n *Node) findData(word []rune, term []rune, tolerance int, exact bool) map[int64]float64 {
	results := make(map[int64]float64)
	stack := [][2]interface{}{{n, word}}

	for len(stack) > 0 {
		currNode, currWord := stack[len(stack)-1][0].(*Node), stack[len(stack)-1][1].([]rune)
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

func mergeNodes(a *Node, b *Node) {
	a.Subword = append(a.Subword, b.Subword...)
	a.Infos = b.Infos
	a.Children = b.Children
}
