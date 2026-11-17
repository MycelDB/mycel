package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
)

// SearchConsistencyMode controls how semantic projection freshness is applied
// to a search request. Strict is the zero-value-compatible default.
type SearchConsistencyMode uint8

const (
	SearchConsistencyStrict SearchConsistencyMode = iota
	SearchConsistencyReadYourWrites
	SearchConsistencyBoundedStale
	SearchConsistencyBestEffort
)

// StaleProjectionBehavior controls the action taken when the current semantic
// projection cannot be certified ready.
type StaleProjectionBehavior uint8

const (
	StaleProjectionFail StaleProjectionBehavior = iota
	StaleProjectionServeLastGood
	StaleProjectionSkipChannel
)

// SearchConsistencyPolicy is scoped to semantic retrieval. Graph visibility,
// authorization, tenant/domain authority, and policy filters remain strict for
// every mode.
type SearchConsistencyPolicy struct {
	Mode                     SearchConsistencyMode
	MinRequiredGraphRevision uint64
	MaxRevisionLag           uint64
	MaxStaleness             time.Duration
	StaleBehavior            StaleProjectionBehavior
}

func (p SearchConsistencyPolicy) IsStrict() bool {
	return p.Mode == SearchConsistencyStrict || p.StaleBehavior == StaleProjectionFail
}

func (p SearchConsistencyPolicy) AllowsLastGood() bool {
	return !p.IsStrict() && p.StaleBehavior == StaleProjectionServeLastGood
}

func (p SearchConsistencyPolicy) AllowsSkip() bool {
	return !p.IsStrict() && p.StaleBehavior == StaleProjectionSkipChannel
}

func (m *Module) staleProjectionFallback(ctx context.Context, in SearchInput, err error) (*semanticsearch.ProjectionFreshness, bool, error) {
	var readiness *ProjectionReadinessError
	if !errors.As(err, &readiness) || !hasTransientProjectionReason(readiness.Reasons) || readiness.SemanticRuleID == uuid.Nil || readiness.EmbeddingBindingKey == "" {
		return nil, false, nil
	}
	status, statusErr := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: readiness.SemanticRuleID, EmbeddingBindingKey: readiness.EmbeddingBindingKey})
	if statusErr != nil {
		return nil, false, statusErr
	}
	rebuildState := strings.ToLower(strings.TrimSpace(status.Rebuild.RebuildState))
	if rebuildState == "required" || rebuildState == "running" || rebuildState == "failed" || strings.EqualFold(status.CapacityState, "exhausted") || (status.RebuildRequired && status.UnresolvedGapCount == 0) {
		return nil, false, nil
	}
	served := status.FreshThroughGraphRevision
	latest := status.LatestObservedGraphRevision
	if served == 0 || (in.Consistency.MinRequiredGraphRevision > 0 && served < in.Consistency.MinRequiredGraphRevision) {
		return nil, false, nil
	}
	lag := uint64(0)
	if latest > served {
		lag = latest - served
	}
	if in.Consistency.MaxRevisionLag > 0 && lag > in.Consistency.MaxRevisionLag {
		return nil, false, nil
	}
	if in.Consistency.MaxStaleness > 0 {
		if status.LastGoodProjectionAt.IsZero() || time.Since(status.LastGoodProjectionAt) > in.Consistency.MaxStaleness {
			return nil, false, nil
		}
	}
	return &semanticsearch.ProjectionFreshness{
		State:                    "stale",
		ServedGraphRevision:      served,
		LatestKnownGraphRevision: latest,
		RevisionLag:              lag,
		Reason:                   "semantic_projection_not_ready",
		Retryable:                true,
	}, true, nil
}

func hasTransientProjectionReason(reasons []string) bool {
	transient := false
	for _, reason := range reasons {
		switch strings.ToLower(strings.TrimSpace(reason)) {
		case ProjectionReadinessCommonRegistry, "configuration_mismatch", "materializer_mismatch", "fencing_mismatch", ProjectionReadinessGenerationMismatch, ProjectionReadinessEpochMismatch:
			return false
		case ProjectionReadinessCommonNotReady, "missing_manifest", "source_lag":
			transient = true
		}
	}
	return transient
}

func (m *Module) semanticSearchFreshness(ctx context.Context, in SearchInput, result semanticsearch.Result) (*semanticsearch.ProjectionFreshness, error) {
	if len(result.ProjectionSearchBindings) == 0 {
		return nil, nil
	}
	var served uint64
	var latest uint64
	for _, binding := range result.ProjectionSearchBindings {
		status, err := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: binding.SemanticRuleID, EmbeddingBindingKey: binding.EmbeddingBindingKey})
		if err != nil {
			return nil, err
		}
		if status.FreshThroughGraphRevision > 0 && (served == 0 || status.FreshThroughGraphRevision < served) {
			served = status.FreshThroughGraphRevision
		}
		if status.LatestObservedGraphRevision > latest {
			latest = status.LatestObservedGraphRevision
		}
	}
	if served == 0 {
		return &semanticsearch.ProjectionFreshness{State: "unavailable", Reason: "semantic_projection_not_ready", Retryable: true}, nil
	}
	lag := uint64(0)
	if latest > served {
		lag = latest - served
	}
	state := "fresh"
	reason := ""
	if lag > 0 {
		state = "stale"
		reason = "dirty_work_pending"
	}
	return &semanticsearch.ProjectionFreshness{State: state, ServedGraphRevision: served, LatestKnownGraphRevision: latest, RevisionLag: lag, Reason: reason}, nil
}
