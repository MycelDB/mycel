package service

import (
	"context"
	"errors"
	"time"

	domaingraph "github.com/myceldb/mycel/internal/graph/model"
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

type GraphChange struct {
	Type   ChangeType
	Node   *domaingraph.Node
	Edge   *domaingraph.Edge
	NodeID string
	EdgeID string
}

type ChangeType string

const (
	ChangeTypeNodeCreated      ChangeType = "node_created"
	ChangeTypeNodeUpdated      ChangeType = "node_updated"
	ChangeTypeNodeDeleted      ChangeType = "node_deleted"
	ChangeTypeEdgeCreated      ChangeType = "edge_created"
	ChangeTypeEdgeUpdated      ChangeType = "edge_updated"
	ChangeTypeEdgeDeleted      ChangeType = "edge_deleted"
	ChangeTypeRevisionAdvanced ChangeType = "revision_advanced"
)
