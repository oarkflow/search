package radix

import (
	"github.com/oarkflow/search/lib"
)

type Trie struct {
	root   *node
	length int
}

func New() *Trie {
	return &Trie{root: newNode(nil)}
}

func (t *Trie) Len() int {
	return t.length
}

func (t *Trie) Insert(id int64, token string, freq float64) {
	word := []rune(token)
	currNode := t.root

	i := 0
	for i < len(word) {
		currentRune := word[i]
		if currChild, ok := currNode.children[currentRune]; ok {
			// Find common prefix length without allocation
			commonPrefix, _ := lib.CommonPrefix(currChild.subword, word[i:])
			commonPrefixLength := len(commonPrefix)
			subwordLength := len(currChild.subword)
			wordLength := len(word[i:])

			if commonPrefixLength == wordLength && commonPrefixLength == subwordLength {
				currChild.addData(id, freq)
				return
			}

			if commonPrefixLength == wordLength && commonPrefixLength < subwordLength {
				n := newNode(word[i:])
				n.addData(id, freq)

				currChild.subword = currChild.subword[commonPrefixLength:]
				n.addChild(currChild)
				currNode.addChild(n)

				t.length++
				return
			}

			if commonPrefixLength < wordLength && commonPrefixLength < subwordLength {
				n := newNode(word[i+commonPrefixLength:])
				n.addData(id, freq)

				inBetweenNode := newNode(word[i : i+commonPrefixLength])
				currNode.addChild(inBetweenNode)

				currChild.subword = currChild.subword[commonPrefixLength:]
				inBetweenNode.addChild(currChild)
				inBetweenNode.addChild(n)

				t.length++
				return
			}

			i += subwordLength
			currNode = currChild
		} else {
			n := newNode(word[i:])
			n.addData(id, freq)

			currNode.addChild(n)
			t.length++
			return
		}
	}
	currNode.putNode()
}

func (t *Trie) Delete(id int64, token string) {
	word := []rune(token)
	currNode := t.root

	for i := 0; i < len(word); {
		char := word[i]
		wordAtIndex := word[i:]

		if currChild, ok := currNode.children[char]; ok {
			if _, eq := lib.CommonPrefix(currChild.subword, wordAtIndex); eq {
				currChild.removeData(id)

				if len(currChild.infos) == 0 {
					switch len(currChild.children) {
					case 0:
						// if the node to be deleted has no children, delete it
						currNode.removeChild(currChild)
						t.length--
					case 1:
						// if the node to be deleted has one child, promote it to the parent node
						for _, child := range currChild.children {
							mergeNodes(currChild, child)
						}
						t.length--
					}
				}
				return
			}

			// skip to the next divergent character
			i += len(currChild.subword)

			// navigate in the child node
			currNode = currChild
		} else {
			// if the node for the curr character doesn't exist abort the deletion
			return
		}
	}
	currNode.putNode()
}

func (t *Trie) Find(token string, tolerance int, exact bool) map[int64]float64 {
	term := []rune(token)
	currNode := t.root
	currNodeWord := currNode.subword

	for i := 0; i < len(term); {
		char := term[i]
		wordAtIndex := term[i:]
		currChild, ok := currNode.children[char]
		if !ok {
			return nil
		}
		commonPrefix, _ := lib.CommonPrefix(currChild.subword, wordAtIndex)
		commonPrefixLength := len(commonPrefix)
		subwordLength := len(currChild.subword)
		wordLength := len(wordAtIndex)

		// if the common prefix length is equal to the node subword length it means they are a match
		// if the common prefix is equal to the term means it is contained in the node
		if commonPrefixLength != wordLength && commonPrefixLength != subwordLength {
			if tolerance > 0 {
				break
			}
			return nil
		}

		// skip to the next divergent character
		i += subwordLength

		// navigate in the child node
		currNode = currChild

		// update the current node word
		currNodeWord = append(currNodeWord, currChild.subword...)
	}

	return currNode.findData(currNodeWord, term, tolerance, exact)
}
