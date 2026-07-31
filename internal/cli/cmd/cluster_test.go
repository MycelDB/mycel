package cmd

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestClusterStatusCommandUsesAdminAPI(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "cluster", "status")
	if err != nil {
		t.Fatalf("cluster status failed: %v\n%s", err, out)
	}
	var status clusterStatusOutput
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatalf("decode cluster status: %v output=%s", err, out)
	}
	if status.Node.NodeID == "" || status.Node.Name == "" || status.Cluster.ClusterID == "" || status.Cluster.Mode == "" {
		t.Fatalf("unexpected cluster status: %#v", status)
	}
	if len(status.Peers) == 0 || status.Peers[0].State != "self" {
		t.Fatalf("expected self peer in status: %#v", status.Peers)
	}
	if !status.Readiness.ClientReady || !status.Readiness.MetadataApplied || !status.Readiness.MetadataValidated || !status.Readiness.PartitionGroupsStarted {
		t.Fatalf("expected readiness fields in cluster status: %#v", status.Readiness)
	}
}

func TestClusterRaftGroupsCommandUsesAdminAPI(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "cluster", "raft-groups")
	if err != nil {
		t.Fatalf("cluster raft-groups failed: %v\n%s", err, out)
	}
	var groups raftGroupsOutput
	if err := json.Unmarshal([]byte(out), &groups); err != nil {
		t.Fatalf("decode raft groups: %v output=%s", err, out)
	}
	if groups.Groups == nil {
		t.Fatalf("expected groups array in output: %s", out)
	}
}

func TestClusterCommandRequiresAdminCredentials(t *testing.T) {
	_, addr, _, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "cluster", "status")
	if err == nil {
		t.Fatalf("expected cluster status without credentials to fail, output=%s", out)
	}
	if !strings.Contains(err.Error(), "--username/-u and --password/-p") {
		t.Fatalf("unexpected error: %v output=%s", err, out)
	}
}
