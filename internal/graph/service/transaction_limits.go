package service

import (
	"encoding/json"
	"fmt"
	"time"

	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const (
	DefaultGraphTransactionMaxOperations   int32         = 4096
	DefaultGraphTransactionMaxCommandBytes int           = 8 * 1024 * 1024
	DefaultGraphTransactionMaxStagedBytes  int64         = 16 * 1024 * 1024
	DefaultGraphTransactionMaxAge          time.Duration = 10 * time.Minute
)

// GraphTransactionLimits bounds the work accumulated by one atomic graph
// transaction. Limits reject work; they never split it into multiple commits.
type GraphTransactionLimits struct {
	MaxOperations   int32
	MaxCommandBytes int
	MaxStagedBytes  int64
	MaxAge          time.Duration
}

func DefaultGraphTransactionLimits() GraphTransactionLimits {
	return GraphTransactionLimits{
		MaxOperations:   DefaultGraphTransactionMaxOperations,
		MaxCommandBytes: DefaultGraphTransactionMaxCommandBytes,
		MaxStagedBytes:  DefaultGraphTransactionMaxStagedBytes,
		MaxAge:          DefaultGraphTransactionMaxAge,
	}
}

func normalizeGraphTransactionLimits(limits GraphTransactionLimits) GraphTransactionLimits {
	defaults := DefaultGraphTransactionLimits()
	if limits.MaxOperations == 0 {
		limits.MaxOperations = defaults.MaxOperations
	}
	if limits.MaxCommandBytes == 0 {
		limits.MaxCommandBytes = defaults.MaxCommandBytes
	}
	if limits.MaxStagedBytes == 0 {
		limits.MaxStagedBytes = defaults.MaxStagedBytes
	}
	if limits.MaxAge == 0 {
		limits.MaxAge = defaults.MaxAge
	}
	return limits
}

// TransactionLimitError is intentionally small and stable enough to map to a
// public ResourceExhausted ErrorInfo reason at the API boundary.
type TransactionLimitError struct {
	Reason string
	Limit  int64
	Actual int64
}

func (e *TransactionLimitError) Error() string {
	return fmt.Sprintf("graph transaction limit exceeded: reason=%s limit=%d actual=%d", e.Reason, e.Limit, e.Actual)
}

func (e *TransactionLimitError) Unwrap() error { return ErrTransactionLimit }

func transactionLimit(reason string, limit, actual int64) error {
	return &TransactionLimitError{Reason: reason, Limit: limit, Actual: actual}
}

func (m *Module) transactionLimitsOrDefault() GraphTransactionLimits {
	return normalizeGraphTransactionLimits(m.transactionLimits)
}

func (m *Module) checkTransactionAge(tx sessionmodel.GraphTransaction) error {
	limits := m.transactionLimitsOrDefault()
	if limits.MaxAge <= 0 || tx.CreatedAt.IsZero() {
		return nil
	}
	age := time.Since(tx.CreatedAt)
	if age > limits.MaxAge {
		return transactionLimit("transaction_age", limits.MaxAge.Milliseconds(), age.Milliseconds())
	}
	return nil
}

func (m *Module) checkStagedMutationLocked(tx sessionmodel.GraphTransaction, o *overlay, additionalBytes int64) error {
	limits := m.transactionLimitsOrDefault()
	nextOperations := int64(o.opCount) + 1
	if limits.MaxOperations > 0 && nextOperations > int64(limits.MaxOperations) {
		return transactionLimit("operation_count", int64(limits.MaxOperations), nextOperations)
	}
	nextBytes := o.stagedBytes + additionalBytes
	if limits.MaxStagedBytes > 0 && nextBytes > limits.MaxStagedBytes {
		return transactionLimit("staged_bytes", limits.MaxStagedBytes, nextBytes)
	}
	return nil
}

func (m *Module) checkGraphCommitRecordBytes(record graphCommitRecord) error {
	limits := m.transactionLimitsOrDefault()
	if limits.MaxCommandBytes <= 0 {
		return nil
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal graph commit for admission: %w", err)
	}
	if len(payload) > limits.MaxCommandBytes {
		return transactionLimit("command_bytes", int64(limits.MaxCommandBytes), int64(len(payload)))
	}
	return nil
}

func (m *Module) checkGraphCommandBytes(commandBytes int) error {
	limits := m.transactionLimitsOrDefault()
	if limits.MaxCommandBytes > 0 && commandBytes > limits.MaxCommandBytes {
		return transactionLimit("command_bytes", int64(limits.MaxCommandBytes), int64(commandBytes))
	}
	return nil
}

func estimateNodeMutationBytes(node domaingraph.Node) (int64, error) {
	payload, err := json.Marshal(node)
	if err != nil {
		return 0, fmt.Errorf("marshal graph node for admission: %w", err)
	}
	return int64(len(payload)), nil
}

func estimateEdgeMutationBytes(edge domaingraph.Edge) (int64, error) {
	payload, err := json.Marshal(edge)
	if err != nil {
		return 0, fmt.Errorf("marshal graph edge for admission: %w", err)
	}
	return int64(len(payload)), nil
}

func estimateNodeDeleteBytes(id domaingraph.NodeID) int64 {
	return int64(len(id.String()))
}

func estimateEdgeDeleteBytes(id domaingraph.EdgeID) int64 {
	return int64(len(id.String()))
}
