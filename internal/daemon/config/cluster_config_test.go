package config

import "testing"

func TestClusterConfigRejectsBootstrapWithSeeds(t *testing.T) {
	cfg := ClusterConfig{Bootstrap: true, SeedPeers: []string{"127.0.0.1:9093"}, DiscoveryInterval: DefaultClusterDiscoveryInterval}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected bootstrap with seeds to fail")
	}
}

func TestClusterConfigAllowsBootstrapWithoutSeeds(t *testing.T) {
	cfg := ClusterConfig{Bootstrap: true, DiscoveryInterval: DefaultClusterDiscoveryInterval}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error=%v", err)
	}
}

func TestClusterConfigRejectsUnknownEngine(t *testing.T) {
	cfg := ClusterConfig{Engine: "bogus", DiscoveryInterval: DefaultClusterDiscoveryInterval}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unknown cluster engine to fail")
	}
}

func TestClusterConfigRejectsReplicaFactorGreaterThanNodeCount(t *testing.T) {
	cfg := ClusterConfig{Engine: "raft", RaftNodeCount: 2, RaftPartitionCount: DefaultClusterRaftPartitionCount, RaftReplicaFactor: 3, DiscoveryInterval: DefaultClusterDiscoveryInterval}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected replica factor greater than node count to fail")
	}
}
