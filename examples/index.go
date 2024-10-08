package main

import (
	"fmt"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/search/radix"
)

func main() {
	id := xid.New().String()
	// Create a new Trie
	trie := radix.New("test", id)

	// Insert some values
	trie.Insert(1, "apple", 3.14)
	trie.Insert(2, "app", 2.71)
	trie.Insert(3, "banana", 1.61)
	trie.Insert(4, "band", 1.41)

	// Save trie to disk
	err := trie.Save()
	if err != nil {
		fmt.Println("Error saving trie:", err)
	}
	loadedTrie, err := radix.NewFromFile("test", id)
	if err != nil {
		fmt.Println("Error loading trie:", err)
	}
	// Get values from the loaded trie
	fmt.Println(loadedTrie.Find("apple", 0, true))  // Output: map[1:3.14]
	fmt.Println(loadedTrie.Find("app", 0, true))    // Output: map[2:2.71]
	fmt.Println(loadedTrie.Find("banana", 0, true)) // Output: map[3:1.61]
	fmt.Println(loadedTrie.Find("band", 0, true))   // Output: map[4:1.41]
	// os.Remove("radix_trie.msgpack")
}
