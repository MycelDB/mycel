package service

import "sync/atomic"

type Metrics struct {
	Processed                      int64 `json:"processed"`
	Succeeded                      int64 `json:"succeeded"`
	Skipped                        int64 `json:"skipped"`
	Failed                         int64 `json:"failed"`
	Retryable                      int64 `json:"retryable"`
	GraphReplayScopes              int64 `json:"graph_replay_scopes"`
	GraphReplayFollowerSkips       int64 `json:"graph_replay_follower_skips"`
	GraphReplayEvents              int64 `json:"graph_replay_events"`
	GraphReplaySkippedEvents       int64 `json:"graph_replay_skipped_events"`
	GraphReplayInvocationsCreated  int64 `json:"graph_replay_invocations_created"`
	GraphReplayInvocationsExisting int64 `json:"graph_replay_invocations_existing"`
	GraphReplayCursorAdvances      int64 `json:"graph_replay_cursor_advances"`
	GraphReplayGaps                int64 `json:"graph_replay_gaps"`
	ClaimReclaims                  int64 `json:"claim_reclaims"`
	ClaimAbandoned                 int64 `json:"claim_abandoned"`
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
	}
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
