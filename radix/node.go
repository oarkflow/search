package radix

import (
	"sort"

	"github.com/oarkflow/search/lib"
)

type RecordInfo struct {
	Id            int64
	TermFrequency float64
}

type node struct {
	subword  []rune
	children map[rune]*node
	infos    []RecordInfo
}

func newNode(subword []rune) *node {
	return &node{
		subword:  subword,
		children: make(map[rune]*node),
		infos:    make([]RecordInfo, 0),
	}
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

func (n *node) addRecordInfo(info RecordInfo) {
	num := len(n.infos)
	idx := sort.Search(num, func(i int) bool {
		return n.infos[i].Id >= info.Id
	})

	n.infos = append(n.infos, RecordInfo{})
	copy(n.infos[idx+1:], n.infos[idx:])
	n.infos[idx] = info
}

func (n *node) removeRecordInfo(id int64) bool {
	num := len(n.infos)
	idx := sort.Search(num, func(i int) bool {
		return n.infos[i].Id >= id
	})

	if idx < num && n.infos[idx].Id == id {
		copy(n.infos[idx:], n.infos[idx+1:])
		n.infos[len(n.infos)-1] = RecordInfo{}
		n.infos = n.infos[:len(n.infos)-1]
		return true
	}
	return false
}

func findAllRecordInfos(n *node, word []rune, term []rune, tolerance int, exact bool) []RecordInfo {
	var results []RecordInfo
	stack := [][2]interface{}{{n, word}}

	for len(stack) > 0 {
		currNode, currWord := stack[len(stack)-1][0].(*node), stack[len(stack)-1][1].([]rune)
		stack = stack[:len(stack)-1]

		if _, eq := lib.CommonPrefix(currWord, term); !eq && exact {
			break
		}

		if tolerance > 0 {
			if _, isBounded := lib.BoundedLevenshtein(currWord, term, tolerance); isBounded {
				results = append(results, currNode.infos...)
			}
		} else {
			results = append(results, currNode.infos...)
		}

		for _, child := range currNode.children {
			stack = append(stack, [2]interface{}{child, append(currWord, child.subword...)})
		}
	}

	return results
}

func mergeNodes(a *node, b *node) {
	a.subword = append(a.subword, b.subword...)
	a.infos = b.infos
	a.children = b.children
}
