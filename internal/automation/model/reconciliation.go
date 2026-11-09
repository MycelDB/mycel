package model

import (
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

const (
	ReconciliationStatusQueued            = "queued"
	ReconciliationStatusRunning           = "running"
	ReconciliationStatusCompleted         = "completed"
	ReconciliationStatusCompletedFailures = "completed_with_failures"
	ReconciliationStatusCanceled          = "canceled"
	ReconciliationStatusFailed            = "failed"
	ReconciliationDefaultBatchSize        = 50
	ReconciliationMaxBatchSize            = 500
	ReconciliationMaxConcurrency          = 32
)

// ReconciliationOperation is the durable, sanitized state of one binding
// reconciliation request. It contains progress only; discovery rows and graph
// content are never persisted here.
type ReconciliationOperation struct {
	ID               string         `json:"operation_id"`
	SpaceID          string         `json:"space_id"`
	DomainID         graph.DomainID `json:"domain_id"`
	BindingID        string         `json:"binding_id"`
	BindingVersion   int            `json:"binding_version"`
	ProcedureID      string         `json:"procedure_id"`
	ProcedureVersion int            `json:"procedure_version"`
	IdempotencyKey   string         `json:"idempotency_key"`
	// RequestedByPrincipalID is the authenticated read authority used for
	// discovery after the durable operation is resumed.
	RequestedByPrincipalID string    `json:"requested_by_principal_id,omitempty"`
	Status                 string    `json:"status"`
	Cursor                 string    `json:"cursor,omitempty"`
	BatchSize              int       `json:"batch_size"`
	MaxConcurrency         int       `json:"max_concurrency,omitempty"`
	ScannedCount           int64     `json:"scanned_count"`
	FreshSkippedCount      int64     `json:"fresh_skipped_count"`
	ScheduledCount         int64     `json:"scheduled_count"`
	CompletedCount         int64     `json:"completed_count"`
	RetryingCount          int64     `json:"retrying_count"`
	FailedCount            int64     `json:"failed_count"`
	FailureCode            string    `json:"failure_code,omitempty"`
	CancelRequested        bool      `json:"cancel_requested,omitempty"`
	DiscoveryComplete      bool      `json:"discovery_complete,omitempty"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
	CompletedAt            time.Time `json:"completed_at,omitempty"`
}

func (o ReconciliationOperation) Terminal() bool {
	switch o.Status {
	case ReconciliationStatusCompleted, ReconciliationStatusCompletedFailures, ReconciliationStatusCanceled, ReconciliationStatusFailed:
		return true
	default:
		return false
	}
}
