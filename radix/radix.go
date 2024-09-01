package radix

import (
	"io"
	"os"

	"github.com/oarkflow/msgpack"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/search/lib"
)

var extension = ".trie"

type Trie struct {
	ID     string
	Prefix string
	Root   *Node
	Length int
}

func New(prefix, id string) *Trie {
	return &Trie{Root: newNode(nil), Prefix: prefix, ID: id}
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
			// Find common prefix length without allocation
			commonPrefix, _ := lib.CommonPrefix(currChild.Subword, word[i:])
			commonPrefixLength := len(commonPrefix)
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
			if _, eq := lib.CommonPrefix(currChild.Subword, wordAtIndex); eq {
				currChild.removeData(id)

				if len(currChild.Infos) == 0 {
					switch len(currChild.Children) {
					case 0:
						// if the node to be deleted has no children, delete it
						currNode.removeChild(currChild)
						t.Length--
					case 1:
						// if the node to be deleted has one child, promote it to the parent node
						for _, child := range currChild.Children {
							mergeNodes(currChild, child)
						}
						t.Length--
					}
				}
				return
			}

			// skip to the next divergent character
			i += len(currChild.Subword)

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
	currNode := t.Root
	currNodeWord := currNode.Subword

	for i := 0; i < len(term); {
		char := term[i]
		wordAtIndex := term[i:]
		currChild, ok := currNode.Children[char]
		if !ok {
			return nil
		}
		commonPrefix, _ := lib.CommonPrefix(currChild.Subword, wordAtIndex)
		commonPrefixLength := len(commonPrefix)
		subwordLength := len(currChild.Subword)
		wordLength := len(wordAtIndex)

		// if the common prefix length is equal to the node subword length it means they are a match
		// if the common prefix is equal to the term means it is contained in the node
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

func (t *Trie) FileName() string {
	return t.Prefix + "_" + t.ID + extension
}

// Save saves the entire trie to a file using MessagePack
func (t *Trie) Save() error {
	filePath := t.FileName()
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := msgpack.NewEncoder(file)
	return encoder.Encode(t.Root)
}

// Load saves the entire trie to a file using MessagePack
func (t *Trie) Load() (*Trie, error) {
	filePath := t.FileName()
	return NewFromFile(t.Prefix, filePath)
}

func NewFromFile(prefix string, filePath string) (*Trie, error) {
	filePath = prefix + "_" + filePath + extension
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return NewFromReader(file, prefix)
}

func NewFromReader(reader io.Reader, prefix string) (*Trie, error) {
	decoder := msgpack.NewDecoder(reader)
	t := New(prefix, xid.New().String())
	err := decoder.Decode(t.Root)
	if err != nil {
		return nil, err
	}
	return t, nil
}
