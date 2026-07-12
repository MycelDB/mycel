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

type DomainSearchMode string

type DomainSemanticMode string

const (
	DomainDiscoveryModeNormal       DomainDiscoveryMode = "normal"
	DomainDiscoveryModeExplicitOnly DomainDiscoveryMode = "explicit_only"
	DomainDiscoveryModeHidden       DomainDiscoveryMode = "hidden"
)

const (
	DomainSearchModeNormal       DomainSearchMode = "normal"
	DomainSearchModeExplicitOnly DomainSearchMode = "explicit_only"
	DomainSearchModeDisabled     DomainSearchMode = "disabled"
)

const (
	DomainSemanticModeNormal       DomainSemanticMode = "normal"
	DomainSemanticModeExplicitOnly DomainSemanticMode = "explicit_only"
	DomainSemanticModeDisabled     DomainSemanticMode = "disabled"
)

func NormalizeDomainDiscoveryMode(mode DomainDiscoveryMode) DomainDiscoveryMode {
	switch mode {
	case "", DomainDiscoveryModeNormal:
		return DomainDiscoveryModeNormal
	case DomainDiscoveryModeExplicitOnly:
		return DomainDiscoveryModeExplicitOnly
	case DomainDiscoveryModeHidden:
		return DomainDiscoveryModeHidden
	default:
		return mode
	}
}

func ValidDomainDiscoveryMode(mode DomainDiscoveryMode) bool {
	switch NormalizeDomainDiscoveryMode(mode) {
	case DomainDiscoveryModeNormal, DomainDiscoveryModeExplicitOnly, DomainDiscoveryModeHidden:
		return true
	default:
		return false
	}
}

func NormalizeDomainSearchMode(mode DomainSearchMode) DomainSearchMode {
	switch mode {
	case "", DomainSearchModeNormal:
		return DomainSearchModeNormal
	case DomainSearchModeExplicitOnly:
		return DomainSearchModeExplicitOnly
	case DomainSearchModeDisabled:
		return DomainSearchModeDisabled
	default:
		return mode
	}
}

func ValidDomainSearchMode(mode DomainSearchMode) bool {
	switch NormalizeDomainSearchMode(mode) {
	case DomainSearchModeNormal, DomainSearchModeExplicitOnly, DomainSearchModeDisabled:
		return true
	default:
		return false
	}
}

func NormalizeDomainSemanticMode(mode DomainSemanticMode) DomainSemanticMode {
	switch mode {
	case "", DomainSemanticModeNormal:
		return DomainSemanticModeNormal
	case DomainSemanticModeExplicitOnly:
		return DomainSemanticModeExplicitOnly
	case DomainSemanticModeDisabled:
		return DomainSemanticModeDisabled
	default:
		return mode
	}
}

func ValidDomainSemanticMode(mode DomainSemanticMode) bool {
	switch NormalizeDomainSemanticMode(mode) {
	case DomainSemanticModeNormal, DomainSemanticModeExplicitOnly, DomainSemanticModeDisabled:
		return true
	default:
		return false
	}
}

func DomainDiscoverable(domain Domain) bool {
	return NormalizeDomainDiscoveryMode(domain.DiscoveryMode) == DomainDiscoveryModeNormal
}

func DomainBroadSearchable(domain Domain) bool {
	return NormalizeDomainSearchMode(domain.SearchMode) == DomainSearchModeNormal
}

func DomainExplicitSearchable(domain Domain) bool {
	return NormalizeDomainSearchMode(domain.SearchMode) != DomainSearchModeDisabled
}

func DomainBroadSemanticSearchable(domain Domain) bool {
	return NormalizeDomainSemanticMode(domain.SemanticMode) == DomainSemanticModeNormal
}

func DomainExplicitSemanticSearchable(domain Domain) bool {
	return NormalizeDomainSemanticMode(domain.SemanticMode) != DomainSemanticModeDisabled
}

func DomainSemanticIndexingEnabled(domain Domain) bool {
	return NormalizeDomainSemanticMode(domain.SemanticMode) != DomainSemanticModeDisabled
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
	SearchMode    DomainSearchMode
	SemanticMode  DomainSemanticMode
	ReadOnly      bool
	Default       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
