package v1

import (
	"math/rand"
	"testing"

	googlebt "github.com/google/btree"
)

// ------------------ Our BPTree Benchmarks ------------------

const numItems = 100000

func genInts(n int) []int {
	rand.Seed(42)
	values := make([]int, n)
	for i := 0; i < n; i++ {
		values[i] = rand.Intn(n * 10)
	}
	return values
}

func BenchmarkBPTree_Insert(b *testing.B) {
	items := genInts(numItems)
	b.ResetTimer()
	tree := NewBPTree[int, int](32, "", 1000)
	for i := 0; i < b.N; i++ {
		for _, v := range items {
			tree.Insert(v, v)
		}
	}
}

func BenchmarkBPTree_Search(b *testing.B) {
	items := genInts(numItems)
	tree := NewBPTree[int, int](32, "", 1000)
	for _, v := range items {
		tree.Insert(v, v)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// search random key from inserted values
		k := items[rand.Intn(len(items))]
		tree.Search(k)
	}
}

// ------------------ Google BTree Benchmarks ------------------

// Define an intItem for google btree.
type googleIntItem int

func (a googleIntItem) Less(b googlebt.Item) bool {
	return a < b.(googleIntItem)
}

func BenchmarkGoogleBTree_Insert(b *testing.B) {
	items := genInts(numItems)
	b.ResetTimer()
	tree := googlebt.New(32) // degree 32
	for i := 0; i < b.N; i++ {
		for _, v := range items {
			tree.ReplaceOrInsert(googleIntItem(v))
		}
	}
}

func BenchmarkGoogleBTree_Search(b *testing.B) {
	items := genInts(numItems)
	tree := googlebt.New(32)
	for _, v := range items {
		tree.ReplaceOrInsert(googleIntItem(v))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := googleIntItem(items[rand.Intn(len(items))])
		_ = tree.Get(k)
	}
}
