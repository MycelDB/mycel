package lexicalstatus

import (
	"context"
	"fmt"

	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
)

// Reader exposes the lexical status capability used by API adapters.
type Reader interface {
	Status(ctx context.Context, spaceID string, domainID string, latestKnownGraphRevision uint64) lexicalservice.Status
}

// GraphRevisionReader exposes the compatibility space-wide revision read.
type GraphRevisionReader interface {
	CurrentRevision(ctx context.Context, spaceID string) (int64, error)
}

// DomainGraphRevisionReader exposes the preferred domain-scoped revision read.
type DomainGraphRevisionReader interface {
	CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error)
}

// Read captures lexical status and reconciles a cursor that appears ahead
// against a graph revision read after the cursor observation.
func Read(ctx context.Context, lexical Reader, graphs GraphRevisionReader, spaceID, domainID string) (lexicalservice.Status, error) {
	latest, err := LatestGraphRevision(ctx, graphs, spaceID, domainID)
	if err != nil {
		return lexicalservice.Status{}, err
	}
	status := lexical.Status(ctx, spaceID, domainID, latest)
	if status.IndexedGraphRevision <= latest {
		return status, nil
	}
	latestAfterCursor, err := LatestGraphRevision(ctx, graphs, spaceID, domainID)
	if err != nil {
		return lexicalservice.Status{}, err
	}
	return lexicalservice.ReconcileStatusLatest(status, latestAfterCursor), nil
}

// LatestGraphRevision reads a domain-scoped revision when supported, retaining
// the existing space-wide fallback for compatible graph manager implementations.
func LatestGraphRevision(ctx context.Context, graphs GraphRevisionReader, spaceID, domainID string) (uint64, error) {
	if graphs == nil {
		return 0, nil
	}
	var (
		revision int64
		err      error
	)
	if domains, ok := graphs.(DomainGraphRevisionReader); ok {
		revision, err = domains.CurrentDomainRevision(ctx, spaceID, domainID)
	} else {
		revision, err = graphs.CurrentRevision(ctx, spaceID)
	}
	if err != nil {
		return 0, err
	}
	if revision < 0 {
		return 0, fmt.Errorf("graph revision is negative: %d", revision)
	}
	return uint64(revision), nil
}
