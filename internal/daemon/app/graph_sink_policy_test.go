package app

import (
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
)

func TestGraphCommitSinkPolicyStandaloneAllowsOnlyLegacySynchronousSinks(t *testing.T) {
	policy := graphCommitSinkPolicyForConfig(config.Config{})
	if policy.Clustered {
		t.Fatal("standalone config should not use clustered sink policy")
	}
	if !reflect.DeepEqual(policy.SynchronousSinks, []string{"graph_change_notification", "semantic"}) {
		t.Fatalf("synchronous sinks=%v", policy.SynchronousSinks)
	}
	if len(policy.RaftApplySinks) != 0 {
		t.Fatalf("raft apply sinks=%v, want none", policy.RaftApplySinks)
	}
	if !reflect.DeepEqual(policy.AsyncGraphConsumers, []string{"lexical"}) {
		t.Fatalf("async consumers=%v", policy.AsyncGraphConsumers)
	}
}

func TestGraphCommitSinkPolicyClusteredAvoidsSecondarySynchronousSinks(t *testing.T) {
	policy := graphCommitSinkPolicyForConfig(config.Config{Cluster: config.ClusterConfig{RaftNodeCount: 1}})
	if !policy.Clustered {
		t.Fatal("raft config should use clustered sink policy")
	}
	if len(policy.SynchronousSinks) != 0 {
		t.Fatalf("clustered synchronous sinks=%v, want none", policy.SynchronousSinks)
	}
	if !reflect.DeepEqual(policy.RaftApplySinks, []string{"graph_change_notification"}) {
		t.Fatalf("raft apply sinks=%v", policy.RaftApplySinks)
	}
	if !reflect.DeepEqual(policy.AsyncGraphConsumers, []string{"lexical", "semantic"}) {
		t.Fatalf("async consumers=%v", policy.AsyncGraphConsumers)
	}
}
