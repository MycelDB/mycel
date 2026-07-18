package clustering

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestSwitchoverIntentSaveLoadClear(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	intent := SwitchoverIntent{OperationID: "switch_1", ClusterID: "cluster_a", OldPrimaryNodeID: "node-a", NewPrimaryNodeID: "node-b", NewAuthority: Authority{Version: AuthorityVersion, ClusterID: "cluster_a", Primary: AuthorityPrimary{NodeID: "node-b"}, AuthorityEpoch: 2, Source: AuthoritySourceManual, UpdatedAt: time.Now()}, Phase: SwitchoverIntentTargetInstalled}
	if err := SaveSwitchoverIntent(ctx, dir, intent); err != nil {
		t.Fatal(err)
	}
	got, ok, err := LoadSwitchoverIntent(ctx, dir)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.OperationID != intent.OperationID || got.Phase != intent.Phase {
		t.Fatalf("got=%#v", got)
	}
	if err := ClearSwitchoverIntent(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(SwitchoverIntentPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("intent still exists err=%v", err)
	}
}

func TestManagerRecoversTargetInstalledSwitchoverIntent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := NewManager(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:1", Bootstrap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	newAuthority := Authority{Version: AuthorityVersion, ClusterID: m.Identity().ClusterID, Primary: AuthorityPrimary{NodeID: "node-b", NodeName: "node-b", BackendAdvertiseAddr: "127.0.0.1:2"}, AuthorityEpoch: 2, Source: AuthoritySourceManual, UpdatedAt: time.Now()}
	intent := SwitchoverIntent{OperationID: "switch_1", ClusterID: m.Identity().ClusterID, OldPrimaryNodeID: m.Identity().NodeID, NewPrimaryNodeID: "node-b", NewAuthority: newAuthority, Phase: SwitchoverIntentTargetInstalled}
	if err := SaveSwitchoverIntent(ctx, dir, intent); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewManager(ctx, Options{DataDir: dir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:1", Bootstrap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := restarted.Authority()
	if !ok {
		t.Fatal("authority missing")
	}
	if a.Primary.NodeID != "node-b" || restarted.LocalRole() != NodeRoleFollower {
		t.Fatalf("authority=%#v role=%s", a, restarted.LocalRole())
	}
	if _, ok, _ := LoadSwitchoverIntent(ctx, dir); ok {
		t.Fatal("intent not cleared")
	}
}
