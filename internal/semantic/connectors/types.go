package connectors

import (
	"context"

	domainsemantic "github.com/myceldb/mycel/domain/semantic"
)

type EmbeddingRequest struct {
	Endpoint   domainsemantic.ModelEndpoint
	Model      domainsemantic.InferenceModel
	Capability domainsemantic.ModelEndpointCapability
	Credential domainsemantic.InferenceCredential
	Secret     string
	Input      string
}

type EmbeddingResponse struct {
	Vector            []float64
	InputTokens       int
	OutputTokens      int
	TotalTokens       int
	TokenCountSource  string
	ProviderRequestID string
}

type Connector interface {
	Embed(ctx context.Context, in EmbeddingRequest) (EmbeddingResponse, error)
}
