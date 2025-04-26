package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/oarkflow/xid"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

type node[K Ordered, V any] struct {
	isLeaf   bool
	keys     []K
	children []*node[K, V]
	values   []V
	next     *node[K, V]
}

type BPTree[K Ordered, V any] struct {
	root  *node[K, V]
	order int
}

func NewBPTree[K Ordered, V any](order int) *BPTree[K, V] {
	t := &BPTree[K, V]{order: order}
	t.root = &node[K, V]{isLeaf: true}
	return t
}

func (t *BPTree[K, V]) Search(key K) (V, bool) {
	n := t.root
	for !n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
		n = n.children[i]
	}
	i := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
	if i < len(n.keys) && n.keys[i] == key {
		return n.values[i], true
	}
	var zero V
	return zero, false
}

func (t *BPTree[K, V]) Insert(key K, value V) {
	newKey, newChild := t.insert(t.root, key, value)
	if newChild != nil {
		t.root = &node[K, V]{isLeaf: false, keys: []K{newKey}, children: []*node[K, V]{t.root, newChild}}
	}
}

func (t *BPTree[K, V]) insert(n *node[K, V], key K, value V) (K, *node[K, V]) {
	if n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
		if i < len(n.keys) && n.keys[i] == key {
			n.values[i] = value
			var zero K
			return zero, nil
		}
		n.keys = append(n.keys, key)
		n.values = append(n.values, value)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.values[i+1:], n.values[i:])
		n.keys[i] = key
		n.values[i] = value
		if len(n.keys) < t.order {
			var zero K
			return zero, nil
		}
		return t.splitLeaf(n)
	}
	i := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
	newKey, newChild := t.insert(n.children[i], key, value)
	if newChild == nil {
		var zero K
		return zero, nil
	}
	j := sort.Search(len(n.keys), func(i int) bool { return newKey < n.keys[i] })
	n.keys = append(n.keys, newKey)
	copy(n.keys[j+1:], n.keys[j:])
	n.keys[j] = newKey
	n.children = append(n.children, nil)
	copy(n.children[j+2:], n.children[j+1:])
	n.children[j+1] = newChild
	if len(n.children) <= t.order {
		var zero K
		return zero, nil
	}
	return t.splitInternal(n)
}

func (t *BPTree[K, V]) splitLeaf(n *node[K, V]) (K, *node[K, V]) {
	mid := (t.order + 1) / 2
	newNode := &node[K, V]{isLeaf: true}
	newNode.keys = append(newNode.keys, n.keys[mid:]...)
	newNode.values = append(newNode.values, n.values[mid:]...)
	n.keys = n.keys[:mid]
	n.values = n.values[:mid]
	newNode.next = n.next
	n.next = newNode
	return newNode.keys[0], newNode
}

func (t *BPTree[K, V]) splitInternal(n *node[K, V]) (K, *node[K, V]) {
	mid := t.order / 2
	newNode := &node[K, V]{isLeaf: false}
	promoted := n.keys[mid]
	newNode.keys = append(newNode.keys, n.keys[mid+1:]...)
	newNode.children = append(newNode.children, n.children[mid+1:]...)
	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]
	return promoted, newNode
}

func (t *BPTree[K, V]) Delete(key K) bool {
	deleted := t.delete(nil, t.root, key, 0)
	if !t.root.isLeaf && len(t.root.keys) == 0 {
		t.root = t.root.children[0]
	}
	return deleted
}

func (t *BPTree[K, V]) delete(parent *node[K, V], n *node[K, V], key K, idx int) bool {
	if n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
		if i >= len(n.keys) || n.keys[i] != key {
			return false
		}
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.values = append(n.values[:i], n.values[i+1:]...)
		if parent != nil && len(n.keys) < (t.order+1)/2 {
			t.rebalance(parent, n, idx)
		}
		return true
	}
	i := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
	if t.delete(n, n.children[i], key, i) {
		if i < len(n.children) && len(n.children[i].keys) < (t.order+1)/2 {
			t.rebalance(n, n.children[i], i)
		}
		return true
	}
	return false
}

func (t *BPTree[K, V]) rebalance(parent, child *node[K, V], idx int) {
	var left, right *node[K, V]
	if idx > 0 {
		left = parent.children[idx-1]
	}
	if idx < len(parent.children)-1 {
		right = parent.children[idx+1]
	}
	minKeys := (t.order + 1) / 2
	if left != nil && len(left.keys) > minKeys {
		if child.isLeaf {
			child.keys = append([]K{left.keys[len(left.keys)-1]}, child.keys...)
			child.values = append([]V{left.values[len(left.values)-1]}, child.values...)
			left.keys = left.keys[:len(left.keys)-1]
			left.values = left.values[:len(left.values)-1]
			parent.keys[idx-1] = child.keys[0]
		} else {
			child.keys = append([]K{parent.keys[idx-1]}, child.keys...)
			child.children = append([]*node[K, V]{left.children[len(left.children)-1]}, child.children...)
			left.keys = left.keys[:len(left.keys)-1]
			left.children = left.children[:len(left.children)-1]
			parent.keys[idx-1] = child.keys[0]
		}
		return
	}
	if right != nil && len(right.keys) > minKeys {
		if child.isLeaf {
			child.keys = append(child.keys, right.keys[0])
			child.values = append(child.values, right.values[0])
			right.keys = right.keys[1:]
			right.values = right.values[1:]
			parent.keys[idx] = right.keys[0]
		} else {
			child.keys = append(child.keys, parent.keys[idx])
			child.children = append(child.children, right.children[0])
			parent.keys[idx] = right.keys[0]
			right.keys = right.keys[1:]
			right.children = right.children[1:]
		}
		return
	}
	if left != nil {
		if child.isLeaf {
			left.keys = append(left.keys, child.keys...)
			left.values = append(left.values, child.values...)
			left.next = child.next
		} else {
			left.keys = append(left.keys, parent.keys[idx-1])
			left.keys = append(left.keys, child.keys...)
			left.children = append(left.children, child.children...)
		}
		parent.keys = append(parent.keys[:idx-1], parent.keys[idx:]...)
		parent.children = append(parent.children[:idx], parent.children[idx+1:]...)
	} else if right != nil {
		if child.isLeaf {
			child.keys = append(child.keys, right.keys...)
			child.values = append(child.values, right.values...)
			child.next = right.next
		} else {
			child.keys = append(child.keys, parent.keys[idx])
			child.keys = append(child.keys, right.keys...)
			child.children = append(child.children, right.children...)
		}
		parent.keys = append(parent.keys[:idx], parent.keys[idx+1:]...)
		parent.children = append(parent.children[:idx+1], parent.children[idx+2:]...)
	}
}

func toString(val any) string {
	switch val := val.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int32, int64, int8, int16, uint, uint32, uint64, uint8, uint16:
		return fmt.Sprintf("%d", val)
	case float32:
		buf := make([]byte, 0, 32)
		buf = strconv.AppendFloat(buf, float64(val), 'f', -1, 64)
		return string(buf)
	case float64:
		buf := make([]byte, 0, 32)
		buf = strconv.AppendFloat(buf, val, 'f', -1, 64)
		return string(buf)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func StoreFromJSON(tree *BPTree[string, map[string]any], file string, keyField string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	for _, rec := range records {
		v, ok := rec[keyField]
		if !ok {
			v = xid.New().String()
		}
		key := toString(v)
		tree.Insert(key, rec)
	}
	return nil
}

func main() {
	tree := NewBPTree[int, string](3)
	tree.Insert(10, "a")
	tree.Insert(20, "b")
	tree.Insert(5, "c")
	tree.Insert(6, "d")
	tree.Insert(12, "e")
	tree.Insert(30, "f")
	tree.Insert(7, "g")
	tree.Insert(17, "h")
	keys := []int{5, 6, 7, 10, 12, 17, 20, 30}
	for _, k := range keys {
		if v, ok := tree.Search(k); ok {
			fmt.Printf("%d: %v\n", k, v)
		} else {
			fmt.Printf("%d not found\n", k)
		}
	}
	tree.Delete(10)
	if _, ok := tree.Search(10); !ok {
		fmt.Println("10 deleted")
	}
	treeStr := NewBPTree[string, map[string]any](2)
	start := time.Now()
	err := StoreFromJSON(treeStr, "charge_master.json", "charge_master_id")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Time taken to store from JSON: %v\n", time.Since(start))
}
