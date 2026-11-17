package client

import (
	"errors"
	"fmt"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func semanticConsistencyPolicyFromProto(in *clientv1.ProjectionConsistencyPolicy) (daemonsemantic.SearchConsistencyPolicy, error) {
	policy := daemonsemantic.SearchConsistencyPolicy{}
	if in == nil {
		return policy, nil
	}
	if in.GetMinRequiredGraphRevision() < 0 || in.GetMaxRevisionLag() < 0 {
		return policy, fmt.Errorf("semantic consistency revision bounds must not be negative")
	}
	policy.MinRequiredGraphRevision = uint64(in.GetMinRequiredGraphRevision())
	policy.MaxRevisionLag = uint64(in.GetMaxRevisionLag())
	if maxStaleness := in.GetMaxStaleness(); maxStaleness != nil {
		if err := maxStaleness.CheckValid(); err != nil {
			return policy, fmt.Errorf("semantic consistency max_staleness is invalid: %w", err)
		}
		policy.MaxStaleness = maxStaleness.AsDuration()
		if policy.MaxStaleness < 0 {
			return policy, fmt.Errorf("semantic consistency max_staleness must not be negative")
		}
	}
	switch in.GetConsistency() {
	case clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_UNSPECIFIED,
		clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_STRICT:
		policy.Mode = daemonsemantic.SearchConsistencyStrict
	case clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_READ_YOUR_WRITES:
		policy.Mode = daemonsemantic.SearchConsistencyReadYourWrites
	case clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_BOUNDED_STALE:
		policy.Mode = daemonsemantic.SearchConsistencyBoundedStale
	case clientv1.ProjectionConsistency_PROJECTION_CONSISTENCY_BEST_EFFORT:
		policy.Mode = daemonsemantic.SearchConsistencyBestEffort
	default:
		return policy, fmt.Errorf("unsupported semantic projection consistency")
	}
	switch in.GetStaleBehavior() {
	case clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_UNSPECIFIED:
		switch policy.Mode {
		case daemonsemantic.SearchConsistencyBestEffort:
			policy.StaleBehavior = daemonsemantic.StaleProjectionSkipChannel
		case daemonsemantic.SearchConsistencyReadYourWrites, daemonsemantic.SearchConsistencyBoundedStale:
			policy.StaleBehavior = daemonsemantic.StaleProjectionServeLastGood
		default:
			policy.StaleBehavior = daemonsemantic.StaleProjectionFail
		}
	case clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_FAIL:
		policy.StaleBehavior = daemonsemantic.StaleProjectionFail
	case clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_SERVE_LAST_GOOD:
		policy.StaleBehavior = daemonsemantic.StaleProjectionServeLastGood
	case clientv1.StaleProjectionBehavior_STALE_PROJECTION_BEHAVIOR_SKIP_CHANNEL:
		policy.StaleBehavior = daemonsemantic.StaleProjectionSkipChannel
	default:
		return policy, fmt.Errorf("unsupported semantic stale projection behavior")
	}
	if policy.Mode == daemonsemantic.SearchConsistencyStrict && policy.StaleBehavior != daemonsemantic.StaleProjectionFail {
		return policy, fmt.Errorf("strict semantic consistency requires stale_projection_behavior=FAIL")
	}
	if policy.Mode == daemonsemantic.SearchConsistencyReadYourWrites && policy.MinRequiredGraphRevision == 0 {
		return policy, fmt.Errorf("read-your-writes semantic consistency requires min_required_graph_revision")
	}
	if policy.Mode == daemonsemantic.SearchConsistencyBoundedStale && policy.MaxRevisionLag == 0 && policy.MaxStaleness == 0 {
		return policy, fmt.Errorf("bounded-stale semantic consistency requires max_revision_lag or max_staleness")
	}
	return policy, nil
}

func isSemanticChannelUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var readiness *daemonsemantic.ProjectionReadinessError
	if errors.As(err, &readiness) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok || (st.Code() != codes.Unavailable && st.Code() != codes.FailedPrecondition) {
		return false
	}
	message := strings.ToLower(st.Message())
	return strings.Contains(message, "semantic projection") || strings.Contains(message, "semantic search service") || strings.Contains(message, "semantic raft partition")
}

func unavailableSemanticFreshness(reason string) *semanticsearch.ProjectionFreshness {
	return &semanticsearch.ProjectionFreshness{State: "unavailable", Reason: reason, Retryable: true}
}

func mapLexicalChannelStatus(in lexicalservice.Status) *clientv1.SearchChannelStatus {
	return &clientv1.SearchChannelStatus{Used: true, State: mapFreshnessState(in.State), ServedGraphRevision: int64(in.IndexedGraphRevision), LatestKnownGraphRevision: int64(in.LatestKnownGraphRevision), RevisionLag: int64(in.RevisionLag), Reason: in.LastError, Retryable: in.State != lexicalservice.StateFresh}
}

func mapSemanticChannelStatus(in *semanticsearch.ProjectionFreshness) *clientv1.SearchChannelStatus {
	if in == nil {
		return nil
	}
	return &clientv1.SearchChannelStatus{Used: in.State != "unavailable", State: mapSemanticFreshnessState(in.State), ServedGraphRevision: int64(in.ServedGraphRevision), LatestKnownGraphRevision: int64(in.LatestKnownGraphRevision), RevisionLag: int64(in.RevisionLag), Reason: in.Reason, Retryable: in.Retryable}
}

func mapSemanticFreshnessState(state string) clientv1.SearchFreshnessState {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "fresh":
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_FRESH
	case "stale":
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_STALE
	case "rebuilding":
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_REBUILDING
	case "unavailable":
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNAVAILABLE
	case "error":
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_ERROR
	default:
		return clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNSPECIFIED
	}
}
