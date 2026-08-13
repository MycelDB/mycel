// Package service exposes identity subsystem runtime services.
package service

import principal "github.com/myceldb/mycel/internal/identity/service/principal"

const PrincipalModuleName = principal.ModuleName

type PrincipalModule = principal.Module
type PrincipalManager = principal.Manager
type Principal = principal.Principal
type PrincipalSummary = principal.PrincipalSummary
type RoleBinding = principal.RoleBinding
type PrincipalCapabilityGrant = principal.CapabilityGrant
type PrincipalAccessScope = principal.AccessScope

const (
	PrincipalStateActive   = principal.PrincipalStateActive
	PrincipalStateDisabled = principal.PrincipalStateDisabled
	PrincipalStateDeleted  = principal.PrincipalStateDeleted
)

func NewPrincipalManager() *PrincipalModule { return principal.NewModule() }
