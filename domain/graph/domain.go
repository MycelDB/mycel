package graph

import (
	"time"

	"github.com/google/uuid"
	domainspace "github.com/myceldb/mycel/domain/space"
)

const (
	// DefaultDomainKey is the generic domain created for every space.
	DefaultDomainKey = "default"
	// DefaultDomainName is the human-readable name for the generic domain.
	DefaultDomainName = "Default"
)

// DomainID identifies a graph domain inside a space.
type DomainID = uuid.UUID

// Domain is a mandatory logical partition inside a space.
//
// Every node belongs to exactly one domain. Domains are used as query,
// indexing, and embedding-policy boundaries while access control remains at
// the space layer.
type Domain struct {
	ID          DomainID
	SpaceID     domainspace.SpaceID
	Key         string
	Name        string
	Description string
	Default     bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
