package internal

import (
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// CreateSpaceInput defines space creation request payload.
type CreateSpaceInput struct {
	AccessToken AccessToken
	Name        string
	// OwnerUserID optionally creates the space for another user. When set to a
	// user other than the authenticated caller, the caller must be allowed to
	// manage system access. The owner is granted admin access to the new space.
	OwnerUserID *identity.UserID
	// OwnerRef is an alternative lookup key for OwnerUserID.
	OwnerRef identity.UserRef
	// DefaultDomainKey optionally chooses the key for the space's initial domain.
	// When omitted, graph.DefaultDomainKey is used.
	DefaultDomainKey string
	// DefaultDomainName optionally chooses the display name for the initial domain.
	DefaultDomainName string
}

// ListSpacesInput defines a space list request payload.
type ListSpacesInput struct {
	AccessToken AccessToken
}

// DeleteSpaceInput defines a hard-delete space request payload.
type DeleteSpaceInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
}

// SpaceInfo is returned after creating or resolving a space.
type SpaceInfo struct {
	OwnerID         identity.UserID     `json:"owner_id"`
	SpaceID         domainspace.SpaceID `json:"space_id"`
	Name            string              `json:"name"`
	DefaultDomainID graph.DomainID      `json:"default_domain_id"`
}
