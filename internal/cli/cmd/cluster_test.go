package cmd

import (
	"encoding/json"
	"os"
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
	if status.Node.NodeID == "" || status.Node.Name == "" || status.Node.Role != "primary" || status.Cluster.ClusterID == "" || status.Cluster.Mode == "" || status.Authority.PrimaryNodeID != status.Node.NodeID || status.Authority.AuthorityEpoch != 1 {
		t.Fatalf("unexpected cluster status: %#v", status)
	}
	if len(status.Peers) == 0 || status.Peers[0].State != "self" {
		t.Fatalf("expected self peer in status: %#v", status.Peers)
	}
}

func TestClusterNodeAddAndMembersUseAdminAPI(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	tokenFile := t.TempDir() + "/node-b.join"
	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "cluster", "node", "add", "node-b", "--token-file", tokenFile)
	if err != nil {
		t.Fatalf("cluster node add failed: %v\n%s", err, out)
	}
	var add clusterNodeAddOutput
	if err := json.Unmarshal([]byte(out), &add); err != nil {
		t.Fatalf("decode add output: %v output=%s", err, out)
	}
	if add.NodeName != "node-b" || add.State != "pending" || add.Token != "" || add.TokenFile != tokenFile || add.TokenID == "" {
		t.Fatalf("unexpected add output: %#v", add)
	}
	tokenRaw, err := os.ReadFile(tokenFile)
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(tokenRaw)), "mycel_join_v1_") {
		t.Fatalf("token file missing token err=%v token=%q", err, tokenRaw)
	}
	if strings.Contains(out, strings.TrimSpace(string(tokenRaw))) {
		t.Fatalf("json output leaked token despite --token-file: %s", out)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "cluster", "members")
	if err != nil {
		t.Fatalf("cluster members failed: %v\n%s", err, out)
	}
	var members clusterMembersOutput
	if err := json.Unmarshal([]byte(out), &members); err != nil {
		t.Fatalf("decode members output: %v output=%s", err, out)
	}
	found := false
	for _, member := range members.Members {
		if member.NodeName == "node-b" {
			found = true
			if member.State != "pending" || member.TokenID != add.TokenID || member.TokenExpiresAt == "" {
				t.Fatalf("unexpected pending member: %#v", member)
			}
		}
	}
	if !found {
		t.Fatalf("node-b not found in members: %#v", members.Members)
	}
	if strings.Contains(out, strings.TrimSpace(string(tokenRaw))) || strings.Contains(out, "hash") {
		t.Fatalf("members output leaked token material: %s", out)
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
