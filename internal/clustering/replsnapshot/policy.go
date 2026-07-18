package replsnapshot

import (
	"path/filepath"
	"strings"
)

type SnapshotPathPolicy struct {
	IncludePrefixes  []string
	PreservePrefixes []string
	ExcludePrefixes  []string
}

func DefaultResyncSnapshotPathPolicy() SnapshotPathPolicy {
	return SnapshotPathPolicy{IncludePrefixes: []string{
		"admins/admins.json",
		"users/users.json",
		"meta/spaces.json",
		"meta/domains.json",
		"meta/access.json",
		"meta/accounting",
		"meta/inference",
		"meta/credentials",
		"meta/secrets",
		"templates",
		"graphs",
		"blobs",
		"blob_meta",
	}, PreservePrefixes: []string{
		"meta/clustering/node.json",
		"meta/clustering/local_state.json",
		"meta/clustering/authority.json",
		"meta/clustering/peers.json",
		"meta/clustering/membership.json",
		"meta/clustering/replication",
		"admins/sessions",
		"users/sessions",
		"wal",
		"log",
		"logs",
	}, ExcludePrefixes: []string{
		"meta/clustering",
		"admins/sessions",
		"users/sessions",
		"meta/wal",
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

func hasPathPrefix(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		prefix = filepath.ToSlash(strings.Trim(prefix, "/"))
		if prefix == "" {
			continue
		}
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func (p SnapshotPathPolicy) IsIncluded(path string) bool {
	clean, ok := CleanSnapshotPath(path)
	if !ok || p.IsExcluded(clean) {
		return false
	}
	return hasPathPrefix(clean, p.IncludePrefixes)
}

func (p SnapshotPathPolicy) IsPreserved(path string) bool {
	clean, ok := CleanSnapshotPath(path)
	if !ok {
		return true
	}
	return hasPathPrefix(clean, p.PreservePrefixes)
}

func (p SnapshotPathPolicy) IsExcluded(path string) bool {
	clean, ok := CleanSnapshotPath(path)
	if !ok {
		return true
	}
	if p.IsPreserved(clean) {
		return true
	}
	return hasPathPrefix(clean, p.ExcludePrefixes)
}
