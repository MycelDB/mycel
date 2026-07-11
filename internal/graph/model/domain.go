package graph

import (
	"time"

	"github.com/google/uuid"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const (
	// DefaultDomainKey is the generic domain created for every space.
	DefaultDomainKey = "default"
	// DefaultDomainName is the human-readable name for the generic domain.
	DefaultDomainName = "Default"
)

type DomainDiscoveryMode string

const (
	DomainDiscoveryModeNormal     DomainDiscoveryMode = "normal"
	DomainDiscoveryModeDirectOnly DomainDiscoveryMode = "direct_only"
)

func NormalizeDomainDiscoveryMode(mode DomainDiscoveryMode) DomainDiscoveryMode {
	switch mode {
	case "", DomainDiscoveryModeNormal:
		return DomainDiscoveryModeNormal
	case DomainDiscoveryModeDirectOnly:
		return DomainDiscoveryModeDirectOnly
	default:
		return mode
	}
}

func ValidDomainDiscoveryMode(mode DomainDiscoveryMode) bool {
	switch NormalizeDomainDiscoveryMode(mode) {
	case DomainDiscoveryModeNormal, DomainDiscoveryModeDirectOnly:
		return true
	default:
		return false
	}
}

// DomainID identifies a graph domain inside a space.
type DomainID = uuid.UUID

// Domain is a mandatory logical partition inside a space.
//
// Every node belongs to exactly one domain. Domains are used as query,
// indexing, and embedding-policy boundaries while access control remains at
// the space layer.
type Domain struct {
	ID            DomainID
	SpaceID       domainspace.SpaceID
	Key           string
	Name          string
	Description   string
	DiscoveryMode DomainDiscoveryMode
	Default       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
