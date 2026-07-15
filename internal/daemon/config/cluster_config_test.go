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
