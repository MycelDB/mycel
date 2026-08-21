package disrupttest

import (
	"strings"
	"testing"
)

func TestRenderManifestsIncludesRaftStatefulSetShape(t *testing.T) {
	cfg := ManifestConfigFromCluster(ClusterConfig{Name: "test-cluster", Namespace: "test-ns", Image: "mycel:test", AdminUsername: "admin", AdminPassword: "pw", EncryptionKey: "enc", BackendToken: "backend", NodeCount: 3, PartitionCount: 16}, "myceld", "myceld", "app=myceld")
	manifest, err := RenderManifests(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"kind: StatefulSet",
		"publishNotReadyAddresses: true",
		"podManagementPolicy: Parallel",
		"replicas: 3",
		"image: mycel:test",
		"MYCELD_CLUSTER_RAFT_NODE_COUNT: \"3\"",
		"MYCELD_CLUSTER_RAFT_REPLICA_FACTOR: \"3\"",
		"MYCELD_CLUSTER_RAFT_PARTITION_COUNT: \"16\"",
		"myceld-0.myceld-headless.test-ns.svc.cluster.local:9091",
		"MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID",
		"cluster readiness check",
		"timeoutSeconds: 10",
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("manifest missing %q:\n%s", want, manifest)
		}
	}
}

func TestManifestConfigBuildsNodeAddresses(t *testing.T) {
	cfg := ManifestConfigFromCluster(ClusterConfig{Name: "c", Namespace: "ns", NodeCount: 3, PartitionCount: 8}, "myceld", "myceld", "app=myceld")
	want := "myceld-0.myceld-headless.ns.svc.cluster.local:9091,myceld-1.myceld-headless.ns.svc.cluster.local:9091,myceld-2.myceld-headless.ns.svc.cluster.local:9091"
	if cfg.NodeAddrs != want {
		t.Fatalf("NodeAddrs = %q, want %q", cfg.NodeAddrs, want)
	}
}
