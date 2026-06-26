package accounting

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
)

type Filter struct {
	From              *time.Time
	To                *time.Time
	PrincipalID       identity.UserID
	SpaceID           domainspace.SpaceID
	DomainID          graph.DomainID
	NodeID            graph.NodeID
	SemanticIndexID   domainsemantic.SemanticIndexID
	Operation         string
	ModelEndpointID   domainsemantic.ModelEndpointID
	ModelID           domainsemantic.InferenceModelID
	CredentialGrantID domainsemantic.CredentialGrantID
	Status            string
	Limit             int
}

type SummaryRow struct {
	Group                  map[string]string `json:"group,omitempty"`
	CallCount              int               `json:"call_count"`
	SuccessCount           int               `json:"success_count"`
	FailedCount            int               `json:"failed_count"`
	InputTokens            int               `json:"input_tokens"`
	OutputTokens           int               `json:"output_tokens"`
	TotalTokens            int               `json:"total_tokens"`
	ProviderReportedTokens int               `json:"provider_reported_tokens"`
	EstimatedTokens        int               `json:"estimated_tokens"`
	UnavailableTokenCount  int               `json:"unavailable_token_count"`
}

type IndexEntry struct {
	EventID   uuid.UUID `json:"event_id"`
	Segment   string    `json:"segment"`
	Line      int       `json:"line"`
	CreatedAt time.Time `json:"created_at"`
}

type Manager interface {
	Init(ctx context.Context, location string) error
	Append(ctx context.Context, event domainsemantic.InferenceUsageEvent) (domainsemantic.InferenceUsageEvent, error)
	List(ctx context.Context, filter Filter) ([]domainsemantic.InferenceUsageEvent, error)
	Summarize(ctx context.Context, filter Filter, groupBy []string) ([]SummaryRow, error)
	RebuildIndexes(ctx context.Context) error
	RebuildRollups(ctx context.Context) error
}

func NewManager() Manager { return &defaultManager{} }
