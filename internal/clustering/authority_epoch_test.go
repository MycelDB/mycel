package clustering

import (
	"context"
	"testing"
	"time"
)

func TestManagerSetAuthorityRejectsStaleEpoch(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(ctx, Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:1", Bootstrap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, ok := m.Authority()
	if !ok {
		t.Fatal("authority missing")
	}
	newer := current
	newer.Primary = AuthorityPrimary{NodeID: "node-b", NodeName: "node-b", BackendAdvertiseAddr: "127.0.0.1:2"}
	newer.AuthorityEpoch = current.AuthorityEpoch + 1
	newer.Source = AuthoritySourceManual
	newer.UpdatedAt = time.Now()
	if err := m.SetAuthority(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := m.SetAuthority(ctx, current); err == nil {
		t.Fatal("expected stale authority rejection")
	}
}

func TestManagerSetAuthorityRejectsSameEpochConflict(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(ctx, Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:1", Bootstrap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := m.Authority()
	conflict := current
	conflict.Primary = AuthorityPrimary{NodeID: "node-b", NodeName: "node-b"}
	if err := m.SetAuthority(ctx, conflict); err == nil {
		t.Fatal("expected same-epoch conflict rejection")
	}
}
