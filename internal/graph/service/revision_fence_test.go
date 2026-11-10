package service

import (
	"context"
	"errors"
	"testing"

	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func TestReadPredicateRevisionFenceUsesClusterStableMAC(t *testing.T) {
	ctx := WithRevisionWitnessKey(context.Background(), []byte("cluster-key"))
	readCtx, recorder := WithReadMetadataRecorder(ctx)
	RecordReadScope(readCtx, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002")
	RecordReadPredicate(readCtx)
	RecordStrongReadContext(readCtx, &StrongReadContext{ObservedRevision: 7, Strong: true})
	metadata := recorder.Summary()
	if metadata == nil || len(metadata.RevisionWitness) == 0 {
		t.Fatal("query predicate read did not produce a revision witness")
	}
	if _, _, _, err := DecodeRevisionWitness(metadata.RevisionWitness, []byte("wrong-key")); err == nil {
		t.Fatal("witness verified with the wrong key")
	}
	if _, _, _, err := DecodeRevisionWitness(metadata.RevisionWitness, []byte("cluster-key")); err != nil {
		t.Fatalf("witness verification error = %v", err)
	}
}

func TestReadPredicateRevisionFenceCapturesDomainRevision(t *testing.T) {
	ctx := context.Background()
	spaceID := "00000000-0000-0000-0000-000000000001"
	domainID := "00000000-0000-0000-0000-000000000002"
	readCtx, recorder := WithReadMetadataRecorder(ctx)
	RecordReadScope(readCtx, spaceID, domainID)
	RecordReadPredicate(readCtx)
	RecordStrongReadContext(readCtx, &StrongReadContext{ObservedRevision: 7, Strong: true})
	metadata := recorder.Summary()
	if metadata == nil || len(metadata.RevisionWitness) == 0 {
		t.Fatal("query predicate read did not produce a revision witness")
	}
	gotSpace, gotDomain, dependencies, err := DecodeRevisionWitness(metadata.RevisionWitness)
	if err != nil {
		t.Fatal(err)
	}
	if gotSpace != spaceID || gotDomain != domainID || len(dependencies) != 1 {
		t.Fatalf("scope/dependencies = %q/%q/%+v", gotSpace, gotDomain, dependencies)
	}
	if dependencies[0].Kind != "domain" || dependencies[0].ID != domainID || dependencies[0].Revision != 7 {
		t.Fatalf("dependency = %+v", dependencies[0])
	}
}

type revisionFenceDependencyValidatorFunc func(context.Context, sessionmodel.GraphTransaction, RevisionDependency) error

func (f revisionFenceDependencyValidatorFunc) ValidateRevisionFenceDependency(ctx context.Context, tx sessionmodel.GraphTransaction, dependency RevisionDependency) error {
	return f(ctx, tx, dependency)
}

func TestRevisionFenceValidatesAuthorizationDependencyAtCommitBoundary(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002", 0)
	dependency := RevisionDependency{Kind: "authorization", ID: tx.PrincipalID, Revision: 7}
	witness := encodeRevisionWitness(tx.SpaceID, tx.DomainID, []RevisionDependency{dependency}, nil)
	m.SetRevisionFenceDependencyValidator(revisionFenceDependencyValidatorFunc(func(context.Context, sessionmodel.GraphTransaction, RevisionDependency) error {
		return ErrRevisionFenceConflict
	}))
	if _, err := m.decodeRevisionFences(ctx, tx, [][]byte{witness}); !errors.Is(err, ErrRevisionFenceConflict) {
		t.Fatalf("decode error = %v, want ErrRevisionFenceConflict", err)
	}
}

func TestCommitTransactionGraphRevisionFenceRejectsChangedReadDependency(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID := "00000000-0000-0000-0000-000000000001"
	domainID := "00000000-0000-0000-0000-000000000002"

	seed := graphTx(spaceID, domainID, 0)
	node, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "before"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CommitTransactionGraph(ctx, seed); err != nil {
		t.Fatal(err)
	}

	readTx := graphTx(spaceID, domainID, 1)
	readCtx, recorder := WithReadMetadataRecorder(ctx)
	RecordReadScope(readCtx, spaceID, domainID)
	if _, err := m.GetNode(readCtx, readTx, node.ID.String()); err != nil {
		t.Fatal(err)
	}
	witness := recorder.Summary().RevisionWitness
	if len(witness) == 0 {
		t.Fatal("read did not produce a revision witness")
	}

	concurrent := graphTx(spaceID, domainID, 1)
	updated := "concurrent"
	if _, err := m.UpdateNode(ctx, concurrent, domaingraph.UpdateNodeInput{NodeID: node.ID.String(), Content: &updated, UpdateMask: []string{"content"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CommitTransactionGraph(ctx, concurrent); err != nil {
		t.Fatal(err)
	}

	base, err := m.CurrentDomainRevision(ctx, spaceID, domainID)
	if err != nil {
		t.Fatal(err)
	}
	stale := graphTx(spaceID, domainID, base)
	derived := "stale derived output"
	if _, err := m.UpdateNode(ctx, stale, domaingraph.UpdateNodeInput{NodeID: node.ID.String(), Content: &derived, UpdateMask: []string{"content"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CommitTransactionGraphWithRevisionFences(ctx, stale, [][]byte{witness}); !errors.Is(err, ErrRevisionFenceConflict) {
		t.Fatalf("commit error = %v, want ErrRevisionFenceConflict", err)
	}

	check := graphTx(spaceID, domainID, base)
	got, err := m.GetNode(ctx, check, node.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "concurrent" {
		t.Fatalf("content = %q, want concurrent", got.Content)
	}
	if check.State != sessionmodel.TransactionStateActive {
		t.Fatalf("unexpected check transaction state: %s", check.State)
	}
}
