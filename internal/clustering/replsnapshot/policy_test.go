package replsnapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultResyncSnapshotPathPolicy(t *testing.T) {
	p := DefaultResyncSnapshotPathPolicy()
	included := []string{"users/users.json", "admins/admins.json", "meta/spaces.json", "meta/domains.json", "meta/access.json", "meta/accounting/manifest.json", "meta/inference/models.json", "templates/t.json", "graphs/space/graph.json", "blobs/space/blob", "blob_meta/space/meta.json"}
	for _, path := range included {
		if !p.IsIncluded(path) {
			t.Fatalf("expected included: %s", path)
		}
	}
	excluded := []string{"unknown.txt", "meta/random.json", "meta/clustering/node.json", "meta/clustering/authority.json", "meta/clustering/replication/progress.json", "meta/wal/progress.json", "wal/0001.wal", "log/myceld.log", "users/sessions/refresh_sessions.json", "admins/sessions/refresh_sessions.json", "../evil"}
	for _, path := range excluded {
		if p.IsIncluded(path) {
			t.Fatalf("expected excluded: %s", path)
		}
	}
}

func TestBuildManifestUsesExplicitManagedRoots(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files := map[string]string{"users/users.json": "{}", "meta/spaces.json": "[]", "meta/clustering/node.json": "{}", "wal/0001.wal": "wal", "unknown.txt": "no"}
	for rel, content := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	m, err := BuildManifest(ctx, root, Manifest{ClusterID: "c", PrimaryNodeID: "p", AuthorityEpoch: 1}, DefaultResyncSnapshotPathPolicy())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range m.Files {
		got[f.Path] = true
	}
	if !got["users/users.json"] || !got["meta/spaces.json"] {
		t.Fatalf("managed files missing: %#v", got)
	}
	for _, rel := range []string{"meta/clustering/node.json", "wal/0001.wal", "unknown.txt"} {
		if got[rel] {
			t.Fatalf("unmanaged file included: %s", rel)
		}
	}
}
