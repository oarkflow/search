package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/oarkflow/msgpack"

	"github.com/oarkflow/search/radix"
)

type node struct {
	Subword  []rune            `msgpack:"subword"`
	Children map[rune]*node    `msgpack:"children"`
	Infos    map[int64]float64 `msgpack:"infos"`
}

var nodePool = sync.Pool{
	New: func() interface{} {
		return &node{
			Children: make(map[rune]*node),
			Infos:    make(map[int64]float64),
		}
	},
}

func newNode(subword []rune) *node {
	n := nodePool.Get().(*node)
	n.Subword = subword
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

func mergeNodes(a *node, b *node) {
	a.Subword = append(a.Subword, b.Subword...)
	a.Infos = b.Infos
	a.Children = b.Children
}

// Serialize serializes the node using MessagePack
func (n *node) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	err := encoder.Encode(n)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize deserializes the node from MessagePack
func (n *node) Deserialize(data []byte) error {
	decoder := msgpack.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(n)
	return err
}

type Trie struct {
	Root   *node
	Length int
}

// New initializes a new Trie
func New() *Trie {
	return &Trie{Root: newNode(nil)}
}

func (t *Trie) Len() int {
	return t.Length
}

func (t *Trie) Insert(id int64, token string, freq float64) {
	word := []rune(token)
	currNode := t.Root

	i := 0
	for i < len(word) {
		currentRune := word[i]
		if currChild, ok := currNode.Children[currentRune]; ok {
			pref, _ := commonPrefix(currChild.Subword, word[i:])
			commonPrefixLength := len(pref)
			subwordLength := len(currChild.Subword)
			wordLength := len(word[i:])
			if commonPrefixLength == wordLength && commonPrefixLength == subwordLength {
				currChild.addData(id, freq)
				return
			}
			if commonPrefixLength == wordLength && commonPrefixLength < subwordLength {
				n := newNode(word[i:])
				n.addData(id, freq)
				currChild.Subword = currChild.Subword[commonPrefixLength:]
				n.addChild(currChild)
				currNode.addChild(n)
				t.Length++
				return
			}
			if commonPrefixLength < wordLength && commonPrefixLength < subwordLength {
				n := newNode(word[i+commonPrefixLength:])
				n.addData(id, freq)
				inBetweenNode := newNode(word[i : i+commonPrefixLength])
				currNode.addChild(inBetweenNode)
				currChild.Subword = currChild.Subword[commonPrefixLength:]
				inBetweenNode.addChild(currChild)
				inBetweenNode.addChild(n)
				t.Length++
				return
			}
			i += subwordLength
			currNode = currChild
		} else {
			n := newNode(word[i:])
			n.addData(id, freq)
			currNode.addChild(n)
			t.Length++
			return
		}
	}
	currNode.putNode()
}

func (t *Trie) Delete(id int64, token string) {
	word := []rune(token)
	currNode := t.Root
	for i := 0; i < len(word); {
		char := word[i]
		wordAtIndex := word[i:]
		if currChild, ok := currNode.Children[char]; ok {
			if _, eq := commonPrefix(currChild.Subword, wordAtIndex); eq {
				currChild.removeData(id)
				if len(currChild.Infos) == 0 {
					switch len(currChild.Children) {
					case 0:
						currNode.removeChild(currChild)
						t.Length--
					case 1:
						for _, child := range currChild.Children {
							mergeNodes(currChild, child)
						}
						t.Length--
					}
				}
				return
			}
			i += len(currChild.Subword)
			currNode = currChild
		} else {
			// if the node for the curr character doesn't exist, abort the deletion
			return
		}
	}
	currNode.putNode()
}

func (t *Trie) Find(token string, tolerance int, exact bool) map[int64]float64 {
	term := []rune(token)
	currNode := t.Root
	currNodeWord := currNode.Subword

	for i := 0; i < len(term); {
		char := term[i]
		wordAtIndex := term[i:]
		currChild, ok := currNode.Children[char]
		if !ok {
			return nil
		}
		pref, _ := commonPrefix(currChild.Subword, wordAtIndex)
		commonPrefixLength := len(pref)
		subwordLength := len(currChild.Subword)
		wordLength := len(wordAtIndex)
		if commonPrefixLength != wordLength && commonPrefixLength != subwordLength {
			if tolerance > 0 {
				break
			}
			return nil
		}
		i += subwordLength
		currNode = currChild
		currNodeWord = append(currNodeWord, currChild.Subword...)
	}

	return currNode.findData(currNodeWord, term, tolerance, exact)
}

func commonPrefix(a, b []rune) ([]rune, bool) {
	length := len(a)
	if len(b) < length {
		length = len(b)
	}
	for i := 0; i < length; i++ {
		if a[i] != b[i] {
			return a[:i], false
		}
	}
	return a[:length], true
}

func (n *node) findData(word []rune, term []rune, tolerance int, exact bool) map[int64]float64 {
	results := make(map[int64]float64)
	stack := [][2]interface{}{{n, word}}

	for len(stack) > 0 {
		currNode, currWord := stack[len(stack)-1][0].(*node), stack[len(stack)-1][1].([]rune)
		stack = stack[:len(stack)-1]

		if _, eq := commonPrefix(currWord, term); !eq && exact {
			break
		}

		if tolerance > 0 {
			// Here, you can implement a bounded Levenshtein distance if required
			mapsCopy(results, currNode.Infos)
		} else {
			mapsCopy(results, currNode.Infos)
		}

		for _, child := range currNode.Children {
			stack = append(stack, [2]interface{}{child, append(currWord, child.Subword...)})
		}
	}
	n.putNode()
	return results
}

func mapsCopy(dst, src map[int64]float64) {
	for k, v := range src {
		dst[k] = v
	}
}

// SaveTrie saves the entire trie to a file using MessagePack
func (t *Trie) SaveTrie(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := msgpack.NewEncoder(file)
	return t.serializeNode(t.Root, encoder)
}

// serializeNode recursively serializes nodes to a file
func (t *Trie) serializeNode(n *node, encoder *msgpack.Encoder) error {
	err := encoder.Encode(n)
	if err != nil {
		return err
	}
	for _, child := range n.Children {
		if err := t.serializeNode(child, encoder); err != nil {
			return err
		}
	}
	return nil
}

// LoadTrie loads a trie from a file using MessagePack
func (t *Trie) LoadTrie(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := msgpack.NewDecoder(file)
	root := newNode(nil)
	t.Root = root
	return t.deserializeNode(root, decoder)
}

// deserializeNode recursively deserializes nodes from a file
func (t *Trie) deserializeNode(n *node, decoder *msgpack.Decoder) error {
	err := decoder.Decode(n)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	for _, child := range n.Children {
		err := t.deserializeNode(child, decoder)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	// Create a new Trie
	trie := radix.New()

	// Insert some values
	trie.Insert(1, "apple", 3.14)
	trie.Insert(2, "app", 2.71)
	trie.Insert(3, "banana", 1.61)
	trie.Insert(4, "band", 1.41)

	// Save trie to disk
	err := trie.SaveTrie("radix_trie.msgpack")
	if err != nil {
		fmt.Println("Error saving trie:", err)
	}

	// Load trie from disk
	loadedTrie := radix.New()
	err = loadedTrie.LoadTrie("radix_trie.msgpack")
	if err != nil {
		fmt.Println("Error loading trie:", err)
	}

	// Get values from the loaded trie
	fmt.Println(loadedTrie.Find("apple", 0, true)) // Output: map[1:3.14]
	fmt.Println(loadedTrie.Find("app", 0, true))   // Output: map[2:2.71]
	fmt.Println(loadedTrie.Find("ban", 0, true))   // Output: map[3:1.61]
	fmt.Println(loadedTrie.Find("band", 0, true))  // Output: map[4:1.41]
}
