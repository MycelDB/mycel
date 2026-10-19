package graphstorage

import (
	"reflect"
	"testing"
)

func TestOrderedTreeInsertUpdateDeleteAndCursorIteration(t *testing.T) {
	tree := newOrderedTree[string, int](
		func(left, right string) bool { return left < right },
		func(key string) uint64 { return hashOrderedBytes([]byte(key)) },
	)
	for index, key := range []string{"d", "b", "f", "a", "c", "e", "g"} {
		tree.Put(key, index)
	}
	tree.Put("c", 99)
	tree.Delete("a")
	tree.Delete("f")
	tree.Delete("missing")

	collect := func(cursor string, hasCursor bool) ([]string, []int) {
		iterator := tree.IteratorAfter(cursor, hasCursor)
		var keys []string
		var values []int
		for iterator.HasNext() {
			key, value, ok := iterator.Next()
			if !ok {
				t.Fatal("iterator reported an item but returned none")
			}
			keys = append(keys, key)
			values = append(values, value)
		}
		return keys, values
	}

	keys, values := collect("", false)
	if !reflect.DeepEqual(keys, []string{"b", "c", "d", "e", "g"}) || !reflect.DeepEqual(values, []int{1, 99, 0, 5, 6}) {
		t.Fatalf("full iteration keys=%v values=%v", keys, values)
	}
	keys, _ = collect("c", true)
	if !reflect.DeepEqual(keys, []string{"d", "e", "g"}) {
		t.Fatalf("cursor iteration keys=%v", keys)
	}

	collectReverse := func(cursor string, hasCursor bool) []string {
		iterator := tree.IteratorBefore(cursor, hasCursor)
		var keys []string
		for iterator.HasNext() {
			key, _, ok := iterator.Next()
			if !ok {
				t.Fatal("reverse iterator reported an item but returned none")
			}
			keys = append(keys, key)
		}
		return keys
	}
	if keys := collectReverse("", false); !reflect.DeepEqual(keys, []string{"g", "e", "d", "c", "b"}) {
		t.Fatalf("reverse iteration keys=%v", keys)
	}
	if keys := collectReverse("e", true); !reflect.DeepEqual(keys, []string{"d", "c", "b"}) {
		t.Fatalf("reverse cursor iteration keys=%v", keys)
	}
}
