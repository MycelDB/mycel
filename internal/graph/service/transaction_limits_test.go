package service

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func TestGraphTransactionLimitsRejectOperationCountWithoutMutation(t *testing.T) {
	m := NewModule().WithTransactionLimits(GraphTransactionLimits{MaxOperations: 1, MaxStagedBytes: 1 << 20, MaxCommandBytes: 1 << 20, MaxAge: time.Hour})
	o := newOverlay()
	tx := sessionmodel.GraphTransaction{ID: "tx-1"}
	node := domaingraph.Node{ID: uuid.New(), DomainID: uuid.New(), Payload: map[string]any{"text": "bounded"}}
	if err := m.checkStagedMutationLocked(tx, o, 1); err != nil {
		t.Fatalf("first mutation admission error=%v", err)
	}
	o.opCount = 1
	if err := m.checkStagedMutationLocked(tx, o, 1); err == nil || !errors.Is(err, ErrTransactionLimit) {
		t.Fatalf("second mutation error=%v, want transaction limit", err)
	}
	if o.opCount != 1 || strings.Contains(node.Payload["text"].(string), "secret") {
		t.Fatalf("limit check changed staged state: %+v", o)
	}
}

func TestGraphTransactionLimitsRejectCommandBytes(t *testing.T) {
	m := NewModule().WithTransactionLimits(GraphTransactionLimits{MaxOperations: 10, MaxCommandBytes: 64, MaxStagedBytes: 1 << 20, MaxAge: time.Hour})
	record := graphCommitRecord{PutNodes: []domaingraph.Node{{
		ID:       uuid.New(),
		DomainID: uuid.New(),
		Payload:  map[string]any{"text": strings.Repeat("x", 256)},
	}}}
	var limitErr *TransactionLimitError
	if err := m.checkGraphCommitRecordBytes(record); !errors.As(err, &limitErr) || limitErr.Reason != "command_bytes" {
		t.Fatalf("error=%v, want command_bytes transaction limit", err)
	}
}

func TestGraphTransactionLimitsRejectExpiredTransaction(t *testing.T) {
	m := NewModule().WithTransactionLimits(GraphTransactionLimits{MaxOperations: 10, MaxCommandBytes: 1 << 20, MaxStagedBytes: 1 << 20, MaxAge: time.Second})
	err := m.checkTransactionAge(sessionmodel.GraphTransaction{CreatedAt: time.Now().Add(-2 * time.Second)})
	var limitErr *TransactionLimitError
	if !errors.As(err, &limitErr) || limitErr.Reason != "transaction_age" {
		t.Fatalf("error=%v, want transaction_age transaction limit", err)
	}
}
