package replsnapshot

import (
	"path/filepath"
	"strings"
)

type SnapshotPathPolicy struct {
	PreservePrefixes []string
	ExcludePrefixes  []string
}

func DefaultResyncSnapshotPathPolicy() SnapshotPathPolicy {
	return SnapshotPathPolicy{PreservePrefixes: []string{
		"meta/clustering/node.json",
		"meta/clustering/local_state.json",
		"meta/clustering/authority.json",
		"meta/clustering/peers.json",
		"meta/clustering/membership.json",
		"meta/clustering/replication",
		"wal",
		"log",
		"logs",
	}, ExcludePrefixes: []string{
		"meta/clustering/replication",
		"wal",
		"log",
		"logs",
	}}
}

func CleanSnapshotPath(path string) (string, bool) {
	path = filepath.ToSlash(strings.TrimSpace(path))
	if path == "" || strings.HasPrefix(path, "/") || path == "." {
		return "", false
	}
	clean := filepath.ToSlash(filepath.Clean(path))
	if clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", false
	}
	return clean, true
}

func (p SnapshotPathPolicy) IsPreserved(path string) bool {
	clean, ok := CleanSnapshotPath(path)
	if !ok {
		return true
	}
	for _, prefix := range p.PreservePrefixes {
		prefix = filepath.ToSlash(strings.Trim(prefix, "/"))
		if clean == prefix || strings.HasPrefix(clean, prefix+"/") {
			return true
		}
	}
	return false
}

func (p SnapshotPathPolicy) IsExcluded(path string) bool {
	clean, ok := CleanSnapshotPath(path)
	if !ok {
		return true
	}
	if p.IsPreserved(clean) {
		return true
	}
	for _, prefix := range p.ExcludePrefixes {
		prefix = filepath.ToSlash(strings.Trim(prefix, "/"))
		if clean == prefix || strings.HasPrefix(clean, prefix+"/") {
			return true
		}
	}
	return false
}
