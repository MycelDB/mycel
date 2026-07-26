package service

import "sync/atomic"

type Metrics struct {
	Processed int64 `json:"processed"`
	Succeeded int64 `json:"succeeded"`
	Skipped   int64 `json:"skipped"`
	Failed    int64 `json:"failed"`
	Retryable int64 `json:"retryable"`
}

func (m *AutomationManager) Metrics() Metrics {
	return Metrics{Processed: atomic.LoadInt64(&m.metricsProcessed), Succeeded: atomic.LoadInt64(&m.metricsSucceeded), Skipped: atomic.LoadInt64(&m.metricsSkipped), Failed: atomic.LoadInt64(&m.metricsFailed), Retryable: atomic.LoadInt64(&m.metricsRetryable)}
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
