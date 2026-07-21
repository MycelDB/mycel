// Package service exposes identity subsystem runtime services.
package service

import (
	daemonadmin "github.com/myceldb/mycel/internal/identity/service/admin"
	daemonuser "github.com/myceldb/mycel/internal/identity/service/user"
)

const (
	UserModuleName  = daemonuser.ModuleName
	AdminModuleName = daemonadmin.ModuleName
)

type UserModule = daemonuser.Module
type UserManager = daemonuser.Manager
type User = daemonuser.User
type UserSummary = daemonuser.UserSummary
type CreateUserInput = daemonuser.CreateUserInput

type AdminModule = daemonadmin.Module
type AdminLister = daemonadmin.AdminLister
type OperatorAuthenticator = daemonadmin.OperatorAuthenticator
type OperatorSessionManager = daemonadmin.OperatorSessionManager
type OperatorAuthManager = daemonadmin.OperatorAuthManager
type OperatorPasswordManager = daemonadmin.OperatorPasswordManager
type OperatorManager = daemonadmin.OperatorManager
type Admin = daemonadmin.Admin
type AdminSummary = daemonadmin.AdminSummary
type AccessScope = daemonadmin.AccessScope
type RoleGrant = daemonadmin.RoleGrant
type CapabilityGrant = daemonadmin.CapabilityGrant

type CreateOperatorInput = daemonadmin.CreateOperatorInput
type UpdateOperatorInput = daemonadmin.UpdateOperatorInput

const (
	UserStateActive   = daemonuser.UserStateActive
	UserStateDisabled = daemonuser.UserStateDisabled
	UserStateDeleted  = daemonuser.UserStateDeleted

	AdminStateActive   = daemonadmin.AdminStateActive
	AdminStateDisabled = daemonadmin.AdminStateDisabled
	AdminStateDeleted  = daemonadmin.AdminStateDeleted
)

func NewUserManager() *UserModule   { return daemonuser.NewModule() }
func NewAdminManager() *AdminModule { return daemonadmin.NewModule() }
