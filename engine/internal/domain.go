package internal

import (
	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// CreateDomainInput defines domain creation request payload.
type CreateDomainInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
	Key         string
	Name        string
	Description string
	Default     bool
}

// ListDomainsInput defines a domain list request payload.
type ListDomainsInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
}

// GetDomainInput resolves a domain by ID or by space/key.
type GetDomainInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
	DomainID    graph.DomainID
	Key         string
}

// SetDomainEmbeddingPolicyInput configures semantic indexing defaults for a domain.
type SetDomainEmbeddingPolicyInput struct {
	AccessToken AccessToken
	Policy      domainembedding.DomainEmbeddingPolicy
}

// GetDomainEmbeddingPolicyInput resolves semantic indexing defaults for a domain.
type GetDomainEmbeddingPolicyInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
	DomainID    graph.DomainID
	Key         string
}
