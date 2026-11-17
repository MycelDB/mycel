package client

import (
	"testing"
	"time"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSemanticConsistencyPolicyFromProtoDefaultsToStrict(t *testing.T) {
	got, err := semanticConsistencyPolicyFromProto(nil)
	if err != nil {
		t.Fatalf("semanticConsistencyPolicyFromProto(nil) error = %v", err)
	}
	if got.Mode != daemonsemantic.SearchConsistencyStrict || got.StaleBehavior != daemonsemantic.StaleProjectionFail {
		t.Fatalf("default policy = %#v, want strict/fail", got)
	}
}

func TestSemanticConsistencyPolicyFromProtoBoundedStale(t *testing.T) {
	got, err := semanticConsistencyPolicyFromProto(&clientv1.ProjectionConsistencyPolicy{
		Consistency:              clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_BOUNDED_STALE,
		MinRequiredGraphRevision: 23,
		MaxRevisionLag:           4,
		MaxStaleness:             durationpb.New(2 * time.Minute),
		StaleBehavior:            clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_SERVE_LAST_GOOD,
	})
	if err != nil {
		t.Fatalf("semanticConsistencyPolicyFromProto() error = %v", err)
	}
	if got.Mode != daemonsemantic.SearchConsistencyBoundedStale || got.StaleBehavior != daemonsemantic.StaleProjectionServeLastGood || got.MinRequiredGraphRevision != 23 || got.MaxRevisionLag != 4 || got.MaxStaleness != 2*time.Minute {
		t.Fatalf("bounded-stale policy = %#v", got)
	}
}

func TestSemanticConsistencyPolicyFromProtoRequiresBounds(t *testing.T) {
	if _, err := semanticConsistencyPolicyFromProto(&clientv1.ProjectionConsistencyPolicy{Consistency: clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_READ_YOUR_WRITES}); err == nil {
		t.Fatal("read-your-writes without a minimum revision returned nil error")
	}
	if _, err := semanticConsistencyPolicyFromProto(&clientv1.ProjectionConsistencyPolicy{Consistency: clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_BOUNDED_STALE}); err == nil {
		t.Fatal("bounded-stale without a freshness bound returned nil error")
	}
}

func TestSemanticConsistencyPolicyFromProtoBestEffortDefaultsToSkip(t *testing.T) {
	got, err := semanticConsistencyPolicyFromProto(&clientv1.ProjectionConsistencyPolicy{Consistency: clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_BEST_EFFORT})
	if err != nil {
		t.Fatalf("semanticConsistencyPolicyFromProto() error = %v", err)
	}
	if got.Mode != daemonsemantic.SearchConsistencyBestEffort || got.StaleBehavior != daemonsemantic.StaleProjectionSkipChannel {
		t.Fatalf("best-effort policy = %#v, want best-effort/skip", got)
	}
}

func TestSemanticConsistencyPolicyFromProtoRejectsNegativeBounds(t *testing.T) {
	if _, err := semanticConsistencyPolicyFromProto(&clientv1.ProjectionConsistencyPolicy{MaxRevisionLag: -1}); err == nil {
		t.Fatal("negative max_revision_lag returned nil error")
	}
	if _, err := semanticConsistencyPolicyFromProto(&clientv1.ProjectionConsistencyPolicy{MinRequiredGraphRevision: -1}); err == nil {
		t.Fatal("negative min_required_graph_revision returned nil error")
	}
}

func TestMapSemanticChannelStatus(t *testing.T) {
	got := mapSemanticChannelStatus(&semanticsearch.ProjectionFreshness{State: "stale", ServedGraphRevision: 23, LatestKnownGraphRevision: 25, RevisionLag: 2, Reason: "dirty_work_pending"})
	if got == nil || got.Used != true || got.State != clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_STALE || got.ServedGraphRevision != 23 || got.RevisionLag != 2 || got.Reason != "dirty_work_pending" {
		t.Fatalf("semantic channel status = %#v", got)
	}
	unavailable := mapSemanticChannelStatus(unavailableSemanticFreshness("missing_manifest"))
	if unavailable == nil || unavailable.Used || unavailable.State != clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNAVAILABLE || unavailable.Reason != "missing_manifest" || !unavailable.Retryable {
		t.Fatalf("unavailable semantic channel status = %#v", unavailable)
	}
}

func TestSemanticConsistencyPolicyFromProtoRejectsStrictServeLastGood(t *testing.T) {
	_, err := semanticConsistencyPolicyFromProto(&clientv1.ProjectionConsistencyPolicy{
		Consistency:   clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_STRICT,
		StaleBehavior: clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_SERVE_LAST_GOOD,
	})
	if err == nil {
		t.Fatal("strict serve-last-good policy returned nil error")
	}
}
