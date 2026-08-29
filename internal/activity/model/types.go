package model

import (
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

const (
	SeverityInfo    = "info"
	SeverityWarning = "warning"
	SeverityError   = "error"

	CategoryLifecycle  = "lifecycle"
	CategoryIdentity   = "identity"
	CategoryAccess     = "access"
	CategorySpace      = "space"
	CategoryDomain     = "domain"
	CategoryBackup     = "backup"
	CategoryCluster    = "cluster"
	CategorySemantic   = "semantic"
	CategoryAutomation = "automation"
	CategoryExternal   = "external"
)

var (
	ErrInvalidEvent = errors.New("invalid activity event")
	ErrNotFound     = errors.New("activity event not found")
)

type Event struct {
	EventID        string           `json:"event_id"`
	OccurredAt     time.Time        `json:"occurred_at"`
	IngestedAt     time.Time        `json:"ingested_at"`
	Severity       string           `json:"severity"`
	Category       string           `json:"category"`
	Type           string           `json:"type"`
	Message        string           `json:"message"`
	Source         Source           `json:"source"`
	Actor          Actor            `json:"actor,omitempty"`
	Resource       Resource         `json:"resource,omitempty"`
	CorrelationID  string           `json:"correlation_id,omitempty"`
	IdempotencyKey string           `json:"idempotency_key,omitempty"`
	Metadata       *structpb.Struct `json:"metadata,omitempty"`
}

type Source struct {
	NodeID    string `json:"node_id,omitempty"`
	NodeName  string `json:"node_name,omitempty"`
	PodName   string `json:"pod_name,omitempty"`
	Component string `json:"component,omitempty"`
	Service   string `json:"service,omitempty"`
}

type Actor struct {
	PrincipalID string `json:"principal_id,omitempty"`
	Username    string `json:"username,omitempty"`
}

type Resource struct {
	Kind string `json:"kind,omitempty"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type ListFilter struct {
	Since            time.Time
	Until            time.Time
	Severities       []string
	Categories       []string
	Types            []string
	SourceNodeID     string
	SourcePodName    string
	SourceComponent  string
	SourceService    string
	ActorPrincipalID string
	ResourceKind     string
	ResourceID       string
	CorrelationID    string
	PageSize         int
	PageToken        string
}

type ListSummary struct {
	TotalCount   uint64
	WarningCount uint64
	ErrorCount   uint64
}

type ListResult struct {
	Events        []Event
	NextPageToken string
	Summary       ListSummary
}
