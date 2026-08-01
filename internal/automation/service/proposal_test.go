package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProposalWritesFailClosedWhenWritesDisallowed(t *testing.T) {
	mgr := NewManager(storage.NewFileStore(t.TempDir())).WithWriteAllowed(func() error {
		return status.Error(codes.Unavailable, "local writes disabled")
	})
	domainID := graph.DomainID(uuid.New())
	if _, err := mgr.CreateProposal(context.Background(), domainID, "inst", "step", nil, "summary"); status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateProposal() error = %v, want Unavailable", err)
	}
	if _, err := mgr.ApproveProposal(context.Background(), domainID, "missing", "reviewer"); status.Code(err) != codes.Unavailable {
		t.Fatalf("ApproveProposal() error = %v, want Unavailable", err)
	}
}

func TestProposalApproveReject(t *testing.T) {
	mgr := NewManager(storage.NewFileStore(t.TempDir()))
	domainID := graph.DomainID(uuid.New())
	proposal, err := mgr.CreateProposal(context.Background(), domainID, "inst", "step", nil, "summary")
	if err != nil {
		t.Fatal(err)
	}
	approved, err := mgr.ApproveProposal(context.Background(), domainID, proposal.ID, "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	if approved.Status != "approved" || approved.Reviewer != "reviewer" {
		t.Fatalf("approved=%+v", approved)
	}
	if _, err := mgr.RejectProposal(context.Background(), domainID, proposal.ID, "reviewer"); err == nil {
		t.Fatal("expected non-pending error")
	}
}
