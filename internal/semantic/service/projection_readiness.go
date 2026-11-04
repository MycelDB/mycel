package service

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ProjectionReadinessMissingLease       = "missing_lease"
	ProjectionReadinessLeaderMismatch     = "leader_mismatch"
	ProjectionReadinessTermMismatch       = "term_mismatch"
	ProjectionReadinessGenerationMismatch = "generation_mismatch"
	ProjectionReadinessEpochMismatch      = "rebuild_epoch_mismatch"
	ProjectionReadinessFreshnessLag       = "fresh_through_lag"
	ProjectionReadinessMissingBaseline    = "missing_local_baseline"
	ProjectionReadinessLeadershipChanged  = "leadership_changed"
	ProjectionReadinessNoLocalLeadership  = "local_leadership_unavailable"
	ProjectionReadinessRebuildRequired    = "rebuild_required"
	ProjectionReadinessRebuildRunning     = "rebuild_running"
	ProjectionReadinessRebuildFailed      = "rebuild_failed"
)

// ProjectionReadinessTuple is the non-content evidence required to certify a
// local semantic projection. FreshThrough is a minimum frontier, not equality.
type ProjectionReadinessTuple struct {
	LeaderNode         uint64
	LeaderTerm         uint64
	SemanticGeneration uint64
	RebuildEpoch       string
	FreshThrough       uint64
}

// ProjectionReadinessError identifies a failed readiness fence without exposing
// graph content, vector payloads, credentials, or local filesystem paths. It does
// not authorize serving an uncertified projection or changing recovery state.
type ProjectionReadinessError struct {
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	LocalNode           uint64
	Expected            ProjectionReadinessTuple
	Observed            *ProjectionReadinessTuple
	Reasons             []string
	RebuildState        string
}

func (e *ProjectionReadinessError) Error() string {
	prefix := "semantic projection is unavailable without local partition leadership"
	if e.SemanticRuleID != uuid.Nil {
		prefix = fmt.Sprintf("semantic projection for rule %s binding %q is not ready for leader %d term %d", e.SemanticRuleID, e.EmbeddingBindingKey, e.Expected.LeaderNode, e.Expected.LeaderTerm)
	}
	observed := "missing"
	if e.Observed != nil {
		observed = fmt.Sprintf("leader=%d term=%d generation=%d rebuild_epoch=%q fresh_through=%d", e.Observed.LeaderNode, e.Observed.LeaderTerm, e.Observed.SemanticGeneration, e.Observed.RebuildEpoch, e.Observed.FreshThrough)
	}
	return fmt.Sprintf("%s (reasons=%s; local_node=%d; expected={leader=%d term=%d generation=%d rebuild_epoch=%q fresh_through=%d}; lease={%s}; rebuild_state=%q)", prefix, strings.Join(e.Reasons, ","), e.LocalNode, e.Expected.LeaderNode, e.Expected.LeaderTerm, e.Expected.SemanticGeneration, e.Expected.RebuildEpoch, e.Expected.FreshThrough, observed, e.RebuildState)
}

// GRPCStatus preserves readiness classification and safe diagnostics through the
// existing cluster-backend forwarding boundary. Both public search adapters
// already preserve categorized statuses rather than flattening them to Internal.
func (e *ProjectionReadinessError) GRPCStatus() *status.Status {
	code := codes.Unavailable
	switch strings.ToLower(strings.TrimSpace(e.RebuildState)) {
	case "required", "failed":
		code = codes.FailedPrecondition
	}
	st := status.New(code, e.Error())
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: "SEMANTIC_PROJECTION_NOT_READY", Domain: "myceldb.io",
		Metadata: e.DiagnosticMetadata(),
	})
	if err != nil {
		return st
	}
	return withDetails
}

// DiagnosticMetadata provides a versioned, content-free diagnostic contract for
// API adapters and operator tooling. Missing lease fields are omitted rather than
// inventing an observed tuple with zero values.
func (e *ProjectionReadinessError) DiagnosticMetadata() map[string]string {
	metadata := map[string]string{
		"schema_version":               "1",
		"readiness_reasons":            strings.Join(e.Reasons, ","),
		"local_node":                   strconv.FormatUint(e.LocalNode, 10),
		"leader_node":                  strconv.FormatUint(e.Expected.LeaderNode, 10),
		"leader_term":                  strconv.FormatUint(e.Expected.LeaderTerm, 10),
		"semantic_generation":          strconv.FormatUint(e.Expected.SemanticGeneration, 10),
		"rebuild_epoch":                e.Expected.RebuildEpoch,
		"fresh_through_graph_revision": strconv.FormatUint(e.Expected.FreshThrough, 10),
		"rebuild_state":                e.RebuildState,
		"lease_present":                strconv.FormatBool(e.Observed != nil),
	}
	if e.SemanticRuleID != uuid.Nil {
		metadata["semantic_rule_id"] = e.SemanticRuleID.String()
	}
	if e.EmbeddingBindingKey != "" {
		metadata["embedding_binding_key"] = e.EmbeddingBindingKey
	}
	if e.Observed != nil {
		metadata["lease_leader_node"] = strconv.FormatUint(e.Observed.LeaderNode, 10)
		metadata["lease_leader_term"] = strconv.FormatUint(e.Observed.LeaderTerm, 10)
		metadata["lease_semantic_generation"] = strconv.FormatUint(e.Observed.SemanticGeneration, 10)
		metadata["lease_rebuild_epoch"] = e.Observed.RebuildEpoch
		metadata["lease_fresh_through_graph_revision"] = strconv.FormatUint(e.Observed.FreshThrough, 10)
	}
	return metadata
}

func projectionLeaseTuple(lease semanticProjectionLease) ProjectionReadinessTuple {
	return ProjectionReadinessTuple{LeaderNode: lease.LeaderNode, LeaderTerm: lease.LeaderTerm, SemanticGeneration: lease.SemanticGeneration, RebuildEpoch: lease.RebuildEpoch, FreshThrough: lease.FreshThrough}
}

func projectionLeaseMismatches(expected, observed ProjectionReadinessTuple) []string {
	var reasons []string
	if observed.LeaderNode != expected.LeaderNode {
		reasons = append(reasons, ProjectionReadinessLeaderMismatch)
	}
	if observed.LeaderTerm != expected.LeaderTerm {
		reasons = append(reasons, ProjectionReadinessTermMismatch)
	}
	if observed.SemanticGeneration != expected.SemanticGeneration {
		reasons = append(reasons, ProjectionReadinessGenerationMismatch)
	}
	if observed.RebuildEpoch != expected.RebuildEpoch {
		reasons = append(reasons, ProjectionReadinessEpochMismatch)
	}
	if observed.FreshThrough < expected.FreshThrough {
		reasons = append(reasons, ProjectionReadinessFreshnessLag)
	}
	return reasons
}

func newProjectionReadinessError(entries []semanticProjectionLease, ruleID domainsemantic.SemanticRuleID, bindingKey string, localNode uint64, expected ProjectionReadinessTuple, rebuildState, recoveryReason string) *ProjectionReadinessError {
	err := &ProjectionReadinessError{SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, LocalNode: localNode, Expected: expected, RebuildState: rebuildState, Reasons: []string{ProjectionReadinessMissingLease}}
	for _, entry := range entries {
		if entry.SemanticRuleID != ruleID || entry.EmbeddingBindingKey != bindingKey {
			continue
		}
		observed := projectionLeaseTuple(entry)
		reasons := projectionLeaseMismatches(expected, observed)
		// Diagnose the closest applicable lease, not an unrelated or older entry
		// when persisted historical entries contain multiple candidates.
		if err.Observed == nil || len(reasons) < len(err.Reasons) {
			err.Observed = &observed
			err.Reasons = reasons
		}
	}
	if recoveryReason != "" {
		err.Reasons = append(err.Reasons, recoveryReason)
	}
	return err
}

type projectionRecoveryResult struct {
	ready         bool
	blockedReason string
}
