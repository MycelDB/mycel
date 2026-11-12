package projection

import (
	"context"
	"errors"
	"fmt"
)

type ChangeKind string

const (
	ChangeUpsert    ChangeKind = "upsert"
	ChangeDelete    ChangeKind = "delete"
	ChangeTombstone ChangeKind = "tombstone"
)

type SourceChange struct {
	Frontier Frontier   `json:"frontier"`
	Key      string     `json:"key"`
	Kind     ChangeKind `json:"kind"`
}

type SourcePage struct {
	Scope   Scope          `json:"scope"`
	After   Frontier       `json:"after"`
	End     Frontier       `json:"end"`
	Changes []SourceChange `json:"changes,omitempty"`
	Next    string         `json:"next,omitempty"`
	HasMore bool           `json:"has_more"`
}

type SourceSnapshot struct {
	Scope    Scope    `json:"scope"`
	Boundary Frontier `json:"boundary"`
	Checksum string   `json:"checksum"`
}

// SourceAdapter exposes only the replay boundary. Projection-specific
// materializers interpret the change keys and payloads at their own boundary.
type SourceAdapter interface {
	ReadPage(context.Context, Scope, Frontier, int) (SourcePage, error)
	ReadSnapshot(context.Context, Scope) (SourceSnapshot, error)
}

type SourceChangeApplier func(context.Context, SourceChange) error

// ReplayFromSnapshot applies each committed page exactly once in adapter order.
// It does not retry opaque cursors: expiry, gaps, malformed pages, or an
// application error are returned to the caller for explicit recovery.
func ReplayFromSnapshot(ctx context.Context, adapter SourceAdapter, snapshot SourceSnapshot, pageSize int, apply SourceChangeApplier) (Frontier, error) {
	if adapter == nil {
		return Frontier{}, errors.New("source adapter is required")
	}
	if apply == nil {
		return Frontier{}, errors.New("source change applier is required")
	}
	if pageSize <= 0 {
		return Frontier{}, errors.New("source page size must be positive")
	}
	if err := ValidateSourceSnapshot(snapshot); err != nil {
		return Frontier{}, err
	}
	after := snapshot.Boundary
	for {
		if err := ctx.Err(); err != nil {
			return after, err
		}
		page, err := adapter.ReadPage(ctx, snapshot.Scope, after, pageSize)
		if err != nil {
			return after, err
		}
		if page.Scope != snapshot.Scope {
			return after, NewMalformedSource(after, page.End)
		}
		if err := ValidateSourcePage(page); err != nil {
			return after, err
		}
		if page.After != after {
			return after, NewSourceGap(after, page.After)
		}
		for _, change := range page.Changes {
			if err := apply(ctx, change); err != nil {
				return after, err
			}
		}
		if !page.HasMore {
			return page.End, nil
		}
		next := Frontier{Revision: page.End.Revision, Cursor: page.Next}
		if next == after {
			return after, NewMalformedSource(after, page.End)
		}
		after = next
	}
}

type FrontierFailureCode string

const (
	FrontierCursorExpired FrontierFailureCode = "cursor_expired"
	FrontierSourceGap     FrontierFailureCode = "source_gap"
	FrontierMalformed     FrontierFailureCode = "malformed_source"
)

var ErrFrontierRequiresRebuild = errors.New("projection source frontier requires rebuild")

type FrontierFailure struct {
	Code      FrontierFailureCode
	Requested Frontier
	Observed  Frontier
}

func (e *FrontierFailure) Error() string {
	if e == nil {
		return ErrFrontierRequiresRebuild.Error()
	}
	return fmt.Sprintf("%s: source frontier requires rebuild", e.Code)
}

func (e *FrontierFailure) Unwrap() error { return ErrFrontierRequiresRebuild }

func NeedsRebuild(err error) bool {
	return errors.Is(err, ErrFrontierRequiresRebuild)
}

func NewCursorExpired(requested, observed Frontier) error {
	return &FrontierFailure{Code: FrontierCursorExpired, Requested: requested, Observed: observed}
}

func NewSourceGap(requested, observed Frontier) error {
	return &FrontierFailure{Code: FrontierSourceGap, Requested: requested, Observed: observed}
}

func NewMalformedSource(requested, observed Frontier) error {
	return &FrontierFailure{Code: FrontierMalformed, Requested: requested, Observed: observed}
}

func ValidateSourcePage(page SourcePage) error {
	if err := page.Scope.validate(); err != nil {
		return err
	}
	if err := page.After.validate(); err != nil {
		return err
	}
	if err := page.End.validate(); err != nil {
		return err
	}
	if page.End.Revision < page.After.Revision {
		return NewMalformedSource(page.After, page.End)
	}
	if page.HasMore {
		if err := validateBounded("next_cursor", page.Next, MaxCursorBytes, false); err != nil {
			return NewMalformedSource(page.After, page.End)
		}
	} else if page.Next != "" {
		return NewMalformedSource(page.After, page.End)
	}
	for _, change := range page.Changes {
		if err := change.Frontier.validate(); err != nil {
			return NewMalformedSource(page.After, page.End)
		}
		if err := validateBounded("change_key", change.Key, MaxKeyBytes, false); err != nil {
			return NewMalformedSource(page.After, page.End)
		}
		if change.Frontier.Revision < page.After.Revision || change.Frontier.Revision > page.End.Revision {
			return NewMalformedSource(page.After, page.End)
		}
		switch change.Kind {
		case ChangeUpsert, ChangeDelete, ChangeTombstone:
		default:
			return NewMalformedSource(page.After, page.End)
		}
	}
	return nil
}

func ValidateSourceSnapshot(snapshot SourceSnapshot) error {
	if err := snapshot.Scope.validate(); err != nil {
		return err
	}
	if err := snapshot.Boundary.validate(); err != nil {
		return err
	}
	return validateBounded("snapshot_checksum", snapshot.Checksum, MaxHashBytes, false)
}

func ValidateReplayStart(snapshot SourceSnapshot, page SourcePage) error {
	if err := ValidateSourceSnapshot(snapshot); err != nil {
		return err
	}
	if err := ValidateSourcePage(page); err != nil {
		return err
	}
	if snapshot.Scope != page.Scope || snapshot.Boundary != page.After {
		return NewSourceGap(snapshot.Boundary, page.After)
	}
	return nil
}

func FrontierAtLeast(observed, required Frontier) bool {
	if observed.Revision > required.Revision {
		return true
	}
	if observed.Revision < required.Revision {
		return false
	}
	if required.Cursor == "" {
		return true
	}
	return observed.Cursor == required.Cursor
}
