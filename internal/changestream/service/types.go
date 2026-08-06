package service

import (
	"context"
	"errors"
	"time"

	graphchange "github.com/myceldb/mycel/internal/graph/change"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
)

const ModuleName = "change_stream"

var (
	ErrInvalidInput = errors.New("invalid change stream input")
	ErrOutOfRange   = errors.New("change stream resume revision is no longer available")
)

type Manager interface {
	CurrentRevision(spaceID string, domainID string) int64
	Subscribe(ctx context.Context, input SubscribeInput) (*Subscription, error)
	PublishCommit(ctx context.Context, commit daemonsession.TransactionCommit, changes []GraphChange)
}

type SubscribeInput struct {
	SpaceID       string
	DomainID      string
	AfterRevision *int64
}

type Subscription struct {
	Events <-chan Event
	Cancel func()
}

type Event struct {
	EventID   string
	SpaceID   string
	DomainID  string
	Revision  int64
	CommitID  string
	EventTime time.Time
	Changes   []GraphChange
}

type GraphChange = graphchange.Change

type ChangeType = graphchange.ChangeType

const (
	ChangeTypeNodeCreated      = graphchange.ChangeTypeNodeCreated
	ChangeTypeNodeUpdated      = graphchange.ChangeTypeNodeUpdated
	ChangeTypeNodeDeleted      = graphchange.ChangeTypeNodeDeleted
	ChangeTypeEdgeCreated      = graphchange.ChangeTypeEdgeCreated
	ChangeTypeEdgeUpdated      = graphchange.ChangeTypeEdgeUpdated
	ChangeTypeEdgeDeleted      = graphchange.ChangeTypeEdgeDeleted
	ChangeTypeRevisionAdvanced = graphchange.ChangeTypeRevisionAdvanced
)
