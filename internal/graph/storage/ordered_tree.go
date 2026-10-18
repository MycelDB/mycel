package graphstorage

// orderedTree is a derived in-memory treap used by cursor scans. Priorities are
// deterministic hashes, so inserts and deletes remain expected O(log n)
// without adding persisted state or write-path sorting.
type orderedTree[K comparable, V any] struct {
	root     *orderedTreeNode[K, V]
	less     func(K, K) bool
	priority func(K) uint64
}

type orderedTreeNode[K comparable, V any] struct {
	key      K
	value    V
	priority uint64
	left     *orderedTreeNode[K, V]
	right    *orderedTreeNode[K, V]
}

type orderedTreeIterator[K comparable, V any] struct {
	stack []*orderedTreeNode[K, V]
}

func newOrderedTree[K comparable, V any](less func(K, K) bool, priority func(K) uint64) *orderedTree[K, V] {
	return &orderedTree[K, V]{less: less, priority: priority}
}

func (t *orderedTree[K, V]) Put(key K, value V) {
	if t == nil {
		return
	}
	t.root = t.put(t.root, key, value)
}

func (t *orderedTree[K, V]) put(node *orderedTreeNode[K, V], key K, value V) *orderedTreeNode[K, V] {
	if node == nil {
		return &orderedTreeNode[K, V]{key: key, value: value, priority: t.priority(key)}
	}
	if t.less(key, node.key) {
		node.left = t.put(node.left, key, value)
		if t.higher(node.left, node) {
			node = rotateOrderedRight(node)
		}
		return node
	}
	if t.less(node.key, key) {
		node.right = t.put(node.right, key, value)
		if t.higher(node.right, node) {
			node = rotateOrderedLeft(node)
		}
		return node
	}
	node.value = value
	return node
}

func (t *orderedTree[K, V]) Delete(key K) {
	if t == nil {
		return
	}
	t.root = t.delete(t.root, key)
}

func (t *orderedTree[K, V]) delete(node *orderedTreeNode[K, V], key K) *orderedTreeNode[K, V] {
	if node == nil {
		return nil
	}
	if t.less(key, node.key) {
		node.left = t.delete(node.left, key)
		return node
	}
	if t.less(node.key, key) {
		node.right = t.delete(node.right, key)
		return node
	}
	return t.merge(node.left, node.right)
}

func (t *orderedTree[K, V]) merge(left, right *orderedTreeNode[K, V]) *orderedTreeNode[K, V] {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if t.higher(left, right) {
		left.right = t.merge(left.right, right)
		return left
	}
	right.left = t.merge(left, right.left)
	return right
}

func (t *orderedTree[K, V]) higher(left, right *orderedTreeNode[K, V]) bool {
	if left.priority != right.priority {
		return left.priority < right.priority
	}
	return t.less(left.key, right.key)
}

func rotateOrderedRight[K comparable, V any](node *orderedTreeNode[K, V]) *orderedTreeNode[K, V] {
	left := node.left
	node.left = left.right
	left.right = node
	return left
}

func rotateOrderedLeft[K comparable, V any](node *orderedTreeNode[K, V]) *orderedTreeNode[K, V] {
	right := node.right
	node.right = right.left
	right.left = node
	return right
}

// IteratorAfter returns an iterator positioned strictly after cursor when
// hasCursor is true, or at the first key otherwise. The caller must retain any
// lock that protects the tree for the iterator's lifetime.
func (t *orderedTree[K, V]) IteratorAfter(cursor K, hasCursor bool) orderedTreeIterator[K, V] {
	iterator := orderedTreeIterator[K, V]{}
	for node := t.root; node != nil; {
		if !hasCursor || t.less(cursor, node.key) {
			iterator.stack = append(iterator.stack, node)
			node = node.left
		} else {
			node = node.right
		}
	}
	return iterator
}

func (i *orderedTreeIterator[K, V]) HasNext() bool {
	return i != nil && len(i.stack) > 0
}

func (i *orderedTreeIterator[K, V]) Next() (K, V, bool) {
	if !i.HasNext() {
		var key K
		var value V
		return key, value, false
	}
	last := len(i.stack) - 1
	node := i.stack[last]
	i.stack = i.stack[:last]
	for child := node.right; child != nil; child = child.left {
		i.stack = append(i.stack, child)
	}
	return node.key, node.value, true
}

func hashOrderedBytes(value []byte) uint64 {
	const (
		offset = uint64(1469598103934665603)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for _, b := range value {
		hash ^= uint64(b)
		hash *= prime
	}
	return hash
}
