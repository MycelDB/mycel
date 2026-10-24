package service

import (
	"context"
	"sync/atomic"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
)

type Metrics struct {
	Processed                                       int64 `json:"processed"`
	Succeeded                                       int64 `json:"succeeded"`
	Skipped                                         int64 `json:"skipped"`
	Failed                                          int64 `json:"failed"`
	Retryable                                       int64 `json:"retryable"`
	GraphReplayScopes                               int64 `json:"graph_replay_scopes"`
	GraphReplayFollowerSkips                        int64 `json:"graph_replay_follower_skips"`
	GraphReplayEvents                               int64 `json:"graph_replay_events"`
	GraphReplaySkippedEvents                        int64 `json:"graph_replay_skipped_events"`
	GraphReplayInvocationsCreated                   int64 `json:"graph_replay_invocations_created"`
	GraphReplayInvocationsExisting                  int64 `json:"graph_replay_invocations_existing"`
	GraphReplayCursorAdvances                       int64 `json:"graph_replay_cursor_advances"`
	GraphReplayGaps                                 int64 `json:"graph_replay_gaps"`
	ClaimReclaims                                   int64 `json:"claim_reclaims"`
	ClaimAbandoned                                  int64 `json:"claim_abandoned"`
	ProcedureInferenceRequested                     int64 `json:"procedure_inference_requested"`
	ProcedureInferenceAuthorized                    int64 `json:"procedure_inference_authorized"`
	ProcedureInferenceStarted                       int64 `json:"procedure_inference_started"`
	ProcedureInferenceTerminal                      int64 `json:"procedure_inference_terminal"`
	ProcedureInferenceAmbiguous                     int64 `json:"procedure_inference_ambiguous"`
	ProcedureInferenceAttemptCapacityRejections     int64 `json:"procedure_inference_attempt_capacity_rejections"`
	ProcedureInferenceChangeCapacityRejections      int64 `json:"procedure_inference_change_capacity_rejections"`
	ProcedureInferenceAttemptsPruned                int64 `json:"procedure_inference_attempts_pruned"`
	ProcedureInferenceChangesPruned                 int64 `json:"procedure_inference_changes_pruned"`
	ProcedureInferenceStateBytesPruned              int64 `json:"procedure_inference_state_bytes_pruned"`
	ProcedureInferenceHistoryBytesPruned            int64 `json:"procedure_inference_history_bytes_pruned"`
	ProcedureInferenceRetentionGapAdvances          int64 `json:"procedure_inference_retention_gap_advances"`
	ProcedureInferenceStateByteCapacityRejections   int64 `json:"procedure_inference_state_byte_capacity_rejections"`
	ProcedureInferenceHistoryByteCapacityRejections int64 `json:"procedure_inference_history_byte_capacity_rejections"`
	ProcedureInferenceAttemptCapacity               int64 `json:"procedure_inference_attempt_capacity"`
	ProcedureInferenceChangeCapacity                int64 `json:"procedure_inference_change_capacity"`
	ProcedureInferenceStateByteCapacity             int64 `json:"procedure_inference_state_byte_capacity"`
	ProcedureInferenceHistoryByteCapacity           int64 `json:"procedure_inference_history_byte_capacity"`
	ProcedureInferenceRetainedAttemptTarget         int64 `json:"procedure_inference_retained_attempt_target"`
	ProcedureInferenceRetainedChangeTarget          int64 `json:"procedure_inference_retained_change_target"`
	ProcedureInferenceRetainedStateByteTarget       int64 `json:"procedure_inference_retained_state_byte_target"`
	ProcedureInferenceRetainedHistoryByteTarget     int64 `json:"procedure_inference_retained_history_byte_target"`
	ProcedureInferenceRetentionAgeSeconds           int64 `json:"procedure_inference_retention_age_seconds"`
	ProcedureInferenceAttemptByteCapacity           int64 `json:"procedure_inference_attempt_byte_capacity"`
	ProcedureInferenceEvidenceStringByteCapacity    int64 `json:"procedure_inference_evidence_string_byte_capacity"`
}

func (m *AutomationManager) Metrics() Metrics {
	return Metrics{
		Processed:                      atomic.LoadInt64(&m.metricsProcessed),
		Succeeded:                      atomic.LoadInt64(&m.metricsSucceeded),
		Skipped:                        atomic.LoadInt64(&m.metricsSkipped),
		Failed:                         atomic.LoadInt64(&m.metricsFailed),
		Retryable:                      atomic.LoadInt64(&m.metricsRetryable),
		GraphReplayScopes:              atomic.LoadInt64(&m.metricsGraphReplayScopes),
		GraphReplayFollowerSkips:       atomic.LoadInt64(&m.metricsGraphReplayFollowerSkips),
		GraphReplayEvents:              atomic.LoadInt64(&m.metricsGraphReplayEvents),
		GraphReplaySkippedEvents:       atomic.LoadInt64(&m.metricsGraphReplaySkippedEvents),
		GraphReplayInvocationsCreated:  atomic.LoadInt64(&m.metricsGraphReplayInvocationsCreated),
		GraphReplayInvocationsExisting: atomic.LoadInt64(&m.metricsGraphReplayInvocationsExisting),
		GraphReplayCursorAdvances:      atomic.LoadInt64(&m.metricsGraphReplayCursorAdvances),
		GraphReplayGaps:                atomic.LoadInt64(&m.metricsGraphReplayGaps),
		ClaimReclaims:                  atomic.LoadInt64(&m.metricsClaimReclaims),
		ClaimAbandoned:                 atomic.LoadInt64(&m.metricsClaimAbandoned),
		ProcedureInferenceRequested:    atomic.LoadInt64(&m.metricsProcedureInferenceRequested),
		ProcedureInferenceAuthorized:   atomic.LoadInt64(&m.metricsProcedureInferenceAuthorized),
		ProcedureInferenceStarted:      atomic.LoadInt64(&m.metricsProcedureInferenceStarted),
		ProcedureInferenceTerminal:     atomic.LoadInt64(&m.metricsProcedureInferenceTerminal),
		ProcedureInferenceAmbiguous:    atomic.LoadInt64(&m.metricsProcedureInferenceAmbiguous),
		ProcedureInferenceAttemptCapacityRejections:     atomic.LoadInt64(&m.metricsProcedureInferenceAttemptCapacityRejections),
		ProcedureInferenceChangeCapacityRejections:      atomic.LoadInt64(&m.metricsProcedureInferenceChangeCapacityRejections),
		ProcedureInferenceAttemptsPruned:                atomic.LoadInt64(&m.metricsProcedureInferenceAttemptsPruned),
		ProcedureInferenceChangesPruned:                 atomic.LoadInt64(&m.metricsProcedureInferenceChangesPruned),
		ProcedureInferenceStateBytesPruned:              atomic.LoadInt64(&m.metricsProcedureInferenceStateBytesPruned),
		ProcedureInferenceHistoryBytesPruned:            atomic.LoadInt64(&m.metricsProcedureInferenceHistoryBytesPruned),
		ProcedureInferenceRetentionGapAdvances:          atomic.LoadInt64(&m.metricsProcedureInferenceRetentionGapAdvances),
		ProcedureInferenceStateByteCapacityRejections:   atomic.LoadInt64(&m.metricsProcedureInferenceStateByteCapacityRejections),
		ProcedureInferenceHistoryByteCapacityRejections: atomic.LoadInt64(&m.metricsProcedureInferenceHistoryByteCapacityRejections),
		ProcedureInferenceAttemptCapacity:               maxProcedureInferenceAttemptsPerSpace,
		ProcedureInferenceChangeCapacity:                maxProcedureInferenceChangesPerSpace,
		ProcedureInferenceStateByteCapacity:             maxProcedureInferenceStateBytes,
		ProcedureInferenceHistoryByteCapacity:           maxProcedureInferenceHistoryBytes,
		ProcedureInferenceRetainedAttemptTarget:         retainedProcedureInferenceAttempts,
		ProcedureInferenceRetainedChangeTarget:          retainedProcedureInferenceChanges,
		ProcedureInferenceRetainedStateByteTarget:       retainedProcedureInferenceStateBytes,
		ProcedureInferenceRetainedHistoryByteTarget:     retainedProcedureInferenceHistoryBytes,
		ProcedureInferenceRetentionAgeSeconds:           int64(procedureInferenceRetentionAge / time.Second),
		ProcedureInferenceAttemptByteCapacity:           automation.ProcedureInferenceAttemptMaxBytes,
		ProcedureInferenceEvidenceStringByteCapacity:    automation.ProcedureInferenceEvidenceStringMaxBytes,
	}
}

type ProcedureInferenceCapacityDiagnostics struct {
	Spaces                      int64 `json:"spaces"`
	RetainedAttempts            int64 `json:"retained_attempts"`
	RetainedChanges             int64 `json:"retained_changes"`
	RetainedStateBytes          int64 `json:"retained_state_bytes"`
	RetainedHistoryBytes        int64 `json:"retained_history_bytes"`
	ActiveAttempts              int64 `json:"active_attempts"`
	AmbiguousAttempts           int64 `json:"ambiguous_attempts"`
	SpacesWithHistoryGap        int64 `json:"spaces_with_history_gap"`
	SpacesWithStateGap          int64 `json:"spaces_with_state_gap"`
	AttemptCapacityPerSpace     int64 `json:"attempt_capacity_per_space"`
	ChangeCapacityPerSpace      int64 `json:"change_capacity_per_space"`
	StateByteCapacityPerSpace   int64 `json:"state_byte_capacity_per_space"`
	HistoryByteCapacityPerSpace int64 `json:"history_byte_capacity_per_space"`
	RetainedAttemptTarget       int64 `json:"retained_attempt_target"`
	RetainedChangeTarget        int64 `json:"retained_change_target"`
	RetainedStateByteTarget     int64 `json:"retained_state_byte_target"`
	RetainedHistoryByteTarget   int64 `json:"retained_history_byte_target"`
	RetentionAgeSeconds         int64 `json:"retention_age_seconds"`
	AttemptByteCapacity         int64 `json:"attempt_byte_capacity"`
	EvidenceStringByteCapacity  int64 `json:"evidence_string_byte_capacity"`
}

type procedureInferenceLedgerSummary struct {
	attempts, changes, stateBytes, historyBytes int64
	active, ambiguous                           int64
	historyGap, stateGap                        int64
}

func summarizeProcedureInferenceLedger(ledger automation.ProcedureInferenceLedger) procedureInferenceLedgerSummary {
	stateBytes, historyBytes := procedureInferenceLedgerBytes(ledger)
	summary := procedureInferenceLedgerSummary{
		attempts:     int64(len(ledger.Attempts)),
		changes:      int64(len(ledger.Changes)),
		stateBytes:   int64(stateBytes),
		historyBytes: int64(historyBytes),
	}
	if ledger.EarliestRetainedSequence > 1 {
		summary.historyGap = 1
	}
	if ledger.EarliestRetainedStateSequence > 1 {
		summary.stateGap = 1
	}
	for _, attempt := range ledger.Attempts {
		if attempt.State == automation.InferenceAttemptStateAmbiguous {
			summary.ambiguous++
		}
		if !automation.IsTerminalProcedureInferenceAttemptState(attempt.State) {
			summary.active++
		}
	}
	return summary
}

// observeProcedureInferenceLedger updates process-local aggregate gauges. The
// caller holds inferenceAttemptMu, so apply performs only bounded per-space work.
func (m *AutomationManager) observeProcedureInferenceLedger(ledger automation.ProcedureInferenceLedger) {
	next := summarizeProcedureInferenceLedger(ledger)
	if m.procedureInferenceObserved == nil {
		m.procedureInferenceObserved = make(map[string]procedureInferenceLedgerSummary)
	}
	previous, exists := m.procedureInferenceObserved[ledger.SpaceID]
	m.procedureInferenceObserved[ledger.SpaceID] = next
	if !exists {
		atomic.AddInt64(&m.metricsProcedureInferenceObservedSpaces, 1)
	}
	atomic.AddInt64(&m.metricsProcedureInferenceRetainedAttempts, next.attempts-previous.attempts)
	atomic.AddInt64(&m.metricsProcedureInferenceRetainedChanges, next.changes-previous.changes)
	atomic.AddInt64(&m.metricsProcedureInferenceRetainedStateBytes, next.stateBytes-previous.stateBytes)
	atomic.AddInt64(&m.metricsProcedureInferenceRetainedHistoryBytes, next.historyBytes-previous.historyBytes)
	atomic.AddInt64(&m.metricsProcedureInferenceActiveAttempts, next.active-previous.active)
	atomic.AddInt64(&m.metricsProcedureInferenceCurrentAmbiguousAttempts, next.ambiguous-previous.ambiguous)
	atomic.AddInt64(&m.metricsProcedureInferenceSpacesWithHistoryGap, next.historyGap-previous.historyGap)
	atomic.AddInt64(&m.metricsProcedureInferenceSpacesWithStateGap, next.stateGap-previous.stateGap)
}

// ProcedureInferenceDiagnostics returns constant-time process-local aggregate
// gauges without loading ledgers or exposing identifiers. A space is included
// after its ledger is first observed by this process.
func (m *AutomationManager) ProcedureInferenceDiagnostics(_ context.Context) (ProcedureInferenceCapacityDiagnostics, error) {
	return ProcedureInferenceCapacityDiagnostics{
		Spaces:                      atomic.LoadInt64(&m.metricsProcedureInferenceObservedSpaces),
		RetainedAttempts:            atomic.LoadInt64(&m.metricsProcedureInferenceRetainedAttempts),
		RetainedChanges:             atomic.LoadInt64(&m.metricsProcedureInferenceRetainedChanges),
		RetainedStateBytes:          atomic.LoadInt64(&m.metricsProcedureInferenceRetainedStateBytes),
		RetainedHistoryBytes:        atomic.LoadInt64(&m.metricsProcedureInferenceRetainedHistoryBytes),
		ActiveAttempts:              atomic.LoadInt64(&m.metricsProcedureInferenceActiveAttempts),
		AmbiguousAttempts:           atomic.LoadInt64(&m.metricsProcedureInferenceCurrentAmbiguousAttempts),
		SpacesWithHistoryGap:        atomic.LoadInt64(&m.metricsProcedureInferenceSpacesWithHistoryGap),
		SpacesWithStateGap:          atomic.LoadInt64(&m.metricsProcedureInferenceSpacesWithStateGap),
		AttemptCapacityPerSpace:     maxProcedureInferenceAttemptsPerSpace,
		ChangeCapacityPerSpace:      maxProcedureInferenceChangesPerSpace,
		StateByteCapacityPerSpace:   maxProcedureInferenceStateBytes,
		HistoryByteCapacityPerSpace: maxProcedureInferenceHistoryBytes,
		RetainedAttemptTarget:       retainedProcedureInferenceAttempts,
		RetainedChangeTarget:        retainedProcedureInferenceChanges,
		RetainedStateByteTarget:     retainedProcedureInferenceStateBytes,
		RetainedHistoryByteTarget:   retainedProcedureInferenceHistoryBytes,
		RetentionAgeSeconds:         int64(procedureInferenceRetentionAge / time.Second),
		AttemptByteCapacity:         automation.ProcedureInferenceAttemptMaxBytes,
		EvidenceStringByteCapacity:  automation.ProcedureInferenceEvidenceStringMaxBytes,
	}, nil
}

func (m *AutomationManager) logProcedureInferenceFixedBounds() {
	if m.logger == nil {
		return
	}
	metrics := m.Metrics()
	m.logger.Info("procedure_inference_capacity_bounds",
		"attempt_capacity_per_space", metrics.ProcedureInferenceAttemptCapacity,
		"change_capacity_per_space", metrics.ProcedureInferenceChangeCapacity,
		"state_byte_capacity_per_space", metrics.ProcedureInferenceStateByteCapacity,
		"history_byte_capacity_per_space", metrics.ProcedureInferenceHistoryByteCapacity,
		"retained_attempt_target", metrics.ProcedureInferenceRetainedAttemptTarget,
		"retained_change_target", metrics.ProcedureInferenceRetainedChangeTarget,
		"retained_state_byte_target", metrics.ProcedureInferenceRetainedStateByteTarget,
		"retained_history_byte_target", metrics.ProcedureInferenceRetainedHistoryByteTarget,
		"retention_age_seconds", metrics.ProcedureInferenceRetentionAgeSeconds,
		"attempt_byte_capacity", metrics.ProcedureInferenceAttemptByteCapacity,
		"evidence_string_byte_capacity", metrics.ProcedureInferenceEvidenceStringByteCapacity,
	)
}

func (m *AutomationManager) logProcedureInferenceDiagnostics(ctx context.Context, reason string) {
	if m.logger == nil {
		return
	}
	now := m.now()
	m.procedureInferenceDiagnosticsLogMu.Lock()
	if m.procedureInferenceDiagnosticsLastLog == nil {
		m.procedureInferenceDiagnosticsLastLog = make(map[string]time.Time)
		m.procedureInferenceDiagnosticsSuppressed = make(map[string]int64)
	}
	last := m.procedureInferenceDiagnosticsLastLog[reason]
	if !last.IsZero() && now.Sub(last) < time.Minute {
		m.procedureInferenceDiagnosticsSuppressed[reason]++
		m.procedureInferenceDiagnosticsLogMu.Unlock()
		return
	}
	suppressed := m.procedureInferenceDiagnosticsSuppressed[reason]
	m.procedureInferenceDiagnosticsSuppressed[reason] = 0
	m.procedureInferenceDiagnosticsLastLog[reason] = now
	m.procedureInferenceDiagnosticsLogMu.Unlock()

	diagnostics, err := m.ProcedureInferenceDiagnostics(ctx)
	if err != nil {
		m.logger.Warn("procedure_inference_capacity_diagnostics", "reason", reason, "suppressed_events", suppressed, "diagnostics_error", "unavailable")
		return
	}
	metrics := m.Metrics()
	m.logger.Warn("procedure_inference_capacity_diagnostics",
		"reason", reason,
		"suppressed_events", suppressed,
		"spaces", diagnostics.Spaces,
		"retained_attempts", diagnostics.RetainedAttempts,
		"retained_changes", diagnostics.RetainedChanges,
		"retained_state_bytes", diagnostics.RetainedStateBytes,
		"retained_history_bytes", diagnostics.RetainedHistoryBytes,
		"active_attempts", diagnostics.ActiveAttempts,
		"ambiguous_attempts", diagnostics.AmbiguousAttempts,
		"spaces_with_history_gap", diagnostics.SpacesWithHistoryGap,
		"spaces_with_state_gap", diagnostics.SpacesWithStateGap,
		"requested", metrics.ProcedureInferenceRequested,
		"authorized", metrics.ProcedureInferenceAuthorized,
		"started", metrics.ProcedureInferenceStarted,
		"terminal", metrics.ProcedureInferenceTerminal,
		"ambiguous", metrics.ProcedureInferenceAmbiguous,
		"attempt_capacity_rejections", metrics.ProcedureInferenceAttemptCapacityRejections,
		"change_capacity_rejections", metrics.ProcedureInferenceChangeCapacityRejections,
		"state_byte_capacity_rejections", metrics.ProcedureInferenceStateByteCapacityRejections,
		"history_byte_capacity_rejections", metrics.ProcedureInferenceHistoryByteCapacityRejections,
		"attempts_pruned", metrics.ProcedureInferenceAttemptsPruned,
		"changes_pruned", metrics.ProcedureInferenceChangesPruned,
		"state_bytes_pruned", metrics.ProcedureInferenceStateBytesPruned,
		"history_bytes_pruned", metrics.ProcedureInferenceHistoryBytesPruned,
		"retention_gap_advances", metrics.ProcedureInferenceRetentionGapAdvances,
	)
}

func (m *AutomationManager) recordMetric(status string) {
	atomic.AddInt64(&m.metricsProcessed, 1)
	switch status {
	case "succeeded":
		atomic.AddInt64(&m.metricsSucceeded, 1)
	case "skipped":
		atomic.AddInt64(&m.metricsSkipped, 1)
	case "failed":
		atomic.AddInt64(&m.metricsFailed, 1)
	case "retryable":
		atomic.AddInt64(&m.metricsRetryable, 1)
	}
}

func (m *AutomationManager) recordGraphReplayScope() {
	atomic.AddInt64(&m.metricsGraphReplayScopes, 1)
}

func (m *AutomationManager) recordGraphReplayFollowerSkip() {
	atomic.AddInt64(&m.metricsGraphReplayFollowerSkips, 1)
}

func (m *AutomationManager) recordGraphReplayEvent() {
	atomic.AddInt64(&m.metricsGraphReplayEvents, 1)
}

func (m *AutomationManager) recordGraphReplaySkippedEvent() {
	atomic.AddInt64(&m.metricsGraphReplaySkippedEvents, 1)
}

func (m *AutomationManager) recordGraphReplayInvocationCreated() {
	atomic.AddInt64(&m.metricsGraphReplayInvocationsCreated, 1)
}

func (m *AutomationManager) recordGraphReplayInvocationExisting() {
	atomic.AddInt64(&m.metricsGraphReplayInvocationsExisting, 1)
}

func (m *AutomationManager) recordGraphReplayCursorAdvance() {
	atomic.AddInt64(&m.metricsGraphReplayCursorAdvances, 1)
}

func (m *AutomationManager) recordGraphReplayGap() {
	atomic.AddInt64(&m.metricsGraphReplayGaps, 1)
}

func (m *AutomationManager) recordClaimReclaim() {
	atomic.AddInt64(&m.metricsClaimReclaims, 1)
}

func (m *AutomationManager) recordClaimAbandoned() {
	atomic.AddInt64(&m.metricsClaimAbandoned, 1)
}

func (m *AutomationManager) recordProcedureInferenceTransition(state string) {
	switch state {
	case automation.InferenceAttemptStateRequested:
		atomic.AddInt64(&m.metricsProcedureInferenceRequested, 1)
	case automation.InferenceAttemptStateAuthorized:
		atomic.AddInt64(&m.metricsProcedureInferenceAuthorized, 1)
	case automation.InferenceAttemptStateStarted:
		atomic.AddInt64(&m.metricsProcedureInferenceStarted, 1)
	case automation.InferenceAttemptStateAmbiguous:
		atomic.AddInt64(&m.metricsProcedureInferenceAmbiguous, 1)
		atomic.AddInt64(&m.metricsProcedureInferenceTerminal, 1)
	default:
		if automation.IsTerminalProcedureInferenceAttemptState(state) {
			atomic.AddInt64(&m.metricsProcedureInferenceTerminal, 1)
		}
	}
}

func (m *AutomationManager) recordProcedureInferenceAttemptCapacityRejection(ctx context.Context) {
	atomic.AddInt64(&m.metricsProcedureInferenceAttemptCapacityRejections, 1)
	m.logProcedureInferenceDiagnostics(ctx, "attempt_capacity_rejection")
}

func (m *AutomationManager) recordProcedureInferenceChangeCapacityRejection(ctx context.Context) {
	atomic.AddInt64(&m.metricsProcedureInferenceChangeCapacityRejections, 1)
	m.logProcedureInferenceDiagnostics(ctx, "change_capacity_rejection")
}

func (m *AutomationManager) recordProcedureInferenceStateByteCapacityRejection(ctx context.Context) {
	atomic.AddInt64(&m.metricsProcedureInferenceStateByteCapacityRejections, 1)
	m.logProcedureInferenceDiagnostics(ctx, "state_byte_capacity_rejection")
}

func (m *AutomationManager) recordProcedureInferenceHistoryByteCapacityRejection(ctx context.Context) {
	atomic.AddInt64(&m.metricsProcedureInferenceHistoryByteCapacityRejections, 1)
	m.logProcedureInferenceDiagnostics(ctx, "history_byte_capacity_rejection")
}

func (m *AutomationManager) recordProcedureInferencePrune(ctx context.Context, stats procedureInferencePruneStats) {
	if stats.Attempts > 0 {
		atomic.AddInt64(&m.metricsProcedureInferenceAttemptsPruned, int64(stats.Attempts))
	}
	if stats.Changes > 0 {
		atomic.AddInt64(&m.metricsProcedureInferenceChangesPruned, int64(stats.Changes))
	}
	if stats.StateBytes > 0 {
		atomic.AddInt64(&m.metricsProcedureInferenceStateBytesPruned, int64(stats.StateBytes))
	}
	if stats.HistoryBytes > 0 {
		atomic.AddInt64(&m.metricsProcedureInferenceHistoryBytesPruned, int64(stats.HistoryBytes))
	}
	if stats.GapAdvanced {
		atomic.AddInt64(&m.metricsProcedureInferenceRetentionGapAdvances, 1)
		m.logProcedureInferenceDiagnostics(ctx, "retention_gap_advance")
	}
}
