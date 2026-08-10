package model

import "reflect"

type IndexDiff struct {
	Added   []IndexDefinition
	Removed []IndexDefinition
	Changed []IndexChange
}

type IndexChange struct {
	Old IndexDefinition
	New IndexDefinition
}

func DiffIndexes(oldSchema, newSchema DomainSchema) IndexDiff {
	oldSchema = oldSchema.Normalize()
	newSchema = newSchema.Normalize()
	oldByName := map[string]IndexDefinition{}
	for _, idx := range oldSchema.Indexes {
		oldByName[idx.Name] = idx
	}
	newByName := map[string]IndexDefinition{}
	for _, idx := range newSchema.Indexes {
		newByName[idx.Name] = idx
	}
	diff := IndexDiff{}
	for name, next := range newByName {
		prev, ok := oldByName[name]
		if !ok {
			diff.Added = append(diff.Added, next)
			continue
		}
		if !reflect.DeepEqual(prev, next) {
			diff.Changed = append(diff.Changed, IndexChange{Old: prev, New: next})
		}
	}
	for name, prev := range oldByName {
		if _, ok := newByName[name]; !ok {
			diff.Removed = append(diff.Removed, prev)
		}
	}
	return diff
}
