package acl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/filestore"
	"github.com/myceldb/mycel/internal/identity/model"
	domainaccess "github.com/myceldb/mycel/internal/space/access"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const accessStoreFile = "access.json"

type accessStore struct {
	SystemRules []domainaccess.SystemAccessRule `json:"system_rules"`
	SpaceRules  []domainaccess.SpaceAccessRule  `json:"space_rules"`
}

type defaultManager struct {
	location          string
	storePath         string
	systemRules       []domainaccess.SystemAccessRule
	spaceRules        []domainaccess.SpaceAccessRule
	indexBySystemUser map[identity.UserID]int
	indexBySpaceUser  map[string]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexBySystemUser: map[identity.UserID]int{}, indexBySpaceUser: map[string]int{}}
}

func (m *defaultManager) Init(ctx context.Context, location string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	if err := os.MkdirAll(location, 0o755); err != nil {
		return err
	}
	m.location = location
	m.storePath = filepath.Join(location, accessStoreFile)
	if _, err := os.Stat(m.storePath); err != nil {
		if os.IsNotExist(err) {
			m.systemRules = []domainaccess.SystemAccessRule{}
			m.spaceRules = []domainaccess.SpaceAccessRule{}
			m.rebuildIndex()
			return m.persist()
		}
		return err
	}
	raw, err := os.ReadFile(m.storePath)
	if err != nil {
		return err
	}
	var store accessStore
	if err := json.Unmarshal(raw, &store); err != nil {
		return err
	}
	m.systemRules = store.SystemRules
	m.spaceRules = store.SpaceRules
	m.rebuildIndex()
	return nil
}

func (m *defaultManager) GrantSystemRole(ctx context.Context, in GrantSystemRoleInput) (domainaccess.SystemAccessRule, error) {
	if err := ctx.Err(); err != nil {
		return domainaccess.SystemAccessRule{}, err
	}
	if in.UserID == uuid.Nil {
		return domainaccess.SystemAccessRule{}, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	roles, err := normalizeSystemRoles(in.Roles)
	if err != nil {
		return domainaccess.SystemAccessRule{}, err
	}
	if idx, ok := m.indexBySystemUser[in.UserID]; ok {
		oldRule := m.systemRules[idx]
		newRule := oldRule
		newRule.Roles = roles
		if hasSystemRole(oldRule.Roles, domainaccess.SystemRoleSuperuser) && !hasSystemRole(newRule.Roles, domainaccess.SystemRoleSuperuser) && m.superuserCountExcluding(in.UserID) == 0 {
			return domainaccess.SystemAccessRule{}, ErrLastSuperuser
		}
		m.systemRules[idx] = newRule
		if err := m.persist(); err != nil {
			m.systemRules[idx] = oldRule
			return domainaccess.SystemAccessRule{}, err
		}
		return cloneSystemRule(newRule), nil
	}
	rule := domainaccess.SystemAccessRule{ID: uuid.New(), UserID: in.UserID, Roles: roles}
	m.systemRules = append(m.systemRules, rule)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.systemRules = m.systemRules[:len(m.systemRules)-1]
		m.rebuildIndex()
		return domainaccess.SystemAccessRule{}, err
	}
	return cloneSystemRule(rule), nil
}

func (m *defaultManager) RevokeSystemRole(ctx context.Context, in RevokeSystemRoleInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.UserID == uuid.Nil {
		return fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexBySystemUser[in.UserID]
	if !ok {
		return ErrRuleNotFound
	}
	rule := m.systemRules[idx]
	if hasSystemRole(rule.Roles, domainaccess.SystemRoleSuperuser) && m.superuserCountExcluding(in.UserID) == 0 {
		return ErrLastSuperuser
	}
	oldRules := append([]domainaccess.SystemAccessRule(nil), m.systemRules...)
	m.systemRules = append(m.systemRules[:idx], m.systemRules[idx+1:]...)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.systemRules = oldRules
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) SystemRolesForUser(ctx context.Context, userID identity.UserID) ([]domainaccess.SystemRole, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if userID == uuid.Nil {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexBySystemUser[userID]
	if !ok {
		return []domainaccess.SystemRole{}, nil
	}
	return append([]domainaccess.SystemRole(nil), m.systemRules[idx].Roles...), nil
}

func (m *defaultManager) SystemRules(ctx context.Context) ([]domainaccess.SystemAccessRule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]domainaccess.SystemAccessRule, 0, len(m.systemRules))
	for _, rule := range m.systemRules {
		out = append(out, cloneSystemRule(rule))
	}
	return out, nil
}

func (m *defaultManager) CanSystem(ctx context.Context, userID identity.UserID, permission domainaccess.SystemPermission) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if userID == uuid.Nil {
		return false, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	if !validSystemPermission(permission) {
		return false, fmt.Errorf("%w: invalid system permission", ErrInvalidInput)
	}
	roles, err := m.SystemRolesForUser(ctx, userID)
	if err != nil {
		return false, err
	}
	for _, role := range roles {
		if domainaccess.RoleAllows(role, permission) {
			return true, nil
		}
	}
	return false, nil
}

func (m *defaultManager) Grant(ctx context.Context, in GrantInput) (domainaccess.SpaceAccessRule, error) {
	if err := ctx.Err(); err != nil {
		return domainaccess.SpaceAccessRule{}, err
	}
	if in.SpaceID == uuid.Nil {
		return domainaccess.SpaceAccessRule{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if in.UserID == uuid.Nil {
		return domainaccess.SpaceAccessRule{}, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	permissions, err := normalizePermissions(in.Permissions)
	if err != nil {
		return domainaccess.SpaceAccessRule{}, err
	}

	key := spaceUserKey(in.SpaceID, in.UserID)
	if idx, ok := m.indexBySpaceUser[key]; ok {
		oldRule := m.spaceRules[idx]
		newRule := oldRule
		newRule.Permissions = permissions
		if hasPermission(oldRule.Permissions, domainaccess.SpacePermissionAdmin) && !hasPermission(newRule.Permissions, domainaccess.SpacePermissionAdmin) && m.adminCountExcluding(in.SpaceID, in.UserID) == 0 {
			return domainaccess.SpaceAccessRule{}, ErrLastAdmin
		}
		m.spaceRules[idx] = newRule
		if err := m.persist(); err != nil {
			m.spaceRules[idx] = oldRule
			return domainaccess.SpaceAccessRule{}, err
		}
		return cloneSpaceRule(newRule), nil
	}

	rule := domainaccess.SpaceAccessRule{ID: uuid.New(), SpaceID: in.SpaceID, UserID: in.UserID, Permissions: permissions}
	m.spaceRules = append(m.spaceRules, rule)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.spaceRules = m.spaceRules[:len(m.spaceRules)-1]
		m.rebuildIndex()
		return domainaccess.SpaceAccessRule{}, err
	}
	return cloneSpaceRule(rule), nil
}

func (m *defaultManager) ApplyGrant(ctx context.Context, rule domainaccess.SpaceAccessRule) (domainaccess.SpaceAccessRule, error) {
	if err := ctx.Err(); err != nil {
		return domainaccess.SpaceAccessRule{}, err
	}
	if rule.ID == uuid.Nil {
		return domainaccess.SpaceAccessRule{}, fmt.Errorf("%w: rule id is required", ErrInvalidInput)
	}
	if rule.SpaceID == uuid.Nil {
		return domainaccess.SpaceAccessRule{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if rule.UserID == uuid.Nil {
		return domainaccess.SpaceAccessRule{}, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	permissions, err := normalizePermissions(rule.Permissions)
	if err != nil {
		return domainaccess.SpaceAccessRule{}, err
	}
	key := spaceUserKey(rule.SpaceID, rule.UserID)
	if idx, ok := m.indexBySpaceUser[key]; ok {
		existing := m.spaceRules[idx]
		if existing.ID == rule.ID {
			oldRule := existing
			existing.Permissions = permissions
			m.spaceRules[idx] = existing
			if err := m.persist(); err != nil {
				m.spaceRules[idx] = oldRule
				return domainaccess.SpaceAccessRule{}, err
			}
			return cloneSpaceRule(existing), nil
		}
		return domainaccess.SpaceAccessRule{}, fmt.Errorf("%w: conflicting space/user rule", ErrInvalidInput)
	}
	rule.Permissions = permissions
	m.spaceRules = append(m.spaceRules, rule)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.spaceRules = m.spaceRules[:len(m.spaceRules)-1]
		m.rebuildIndex()
		return domainaccess.SpaceAccessRule{}, err
	}
	return cloneSpaceRule(rule), nil
}

func (m *defaultManager) Revoke(ctx context.Context, in RevokeInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.SpaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if in.UserID == uuid.Nil {
		return fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexBySpaceUser[spaceUserKey(in.SpaceID, in.UserID)]
	if !ok {
		return ErrRuleNotFound
	}
	rule := m.spaceRules[idx]
	if hasPermission(rule.Permissions, domainaccess.SpacePermissionAdmin) && m.adminCountExcluding(in.SpaceID, in.UserID) == 0 {
		return ErrLastAdmin
	}
	oldRules := append([]domainaccess.SpaceAccessRule(nil), m.spaceRules...)
	m.spaceRules = append(m.spaceRules[:idx], m.spaceRules[idx+1:]...)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.spaceRules = oldRules
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) DeleteForUser(ctx context.Context, userID identity.UserID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if userID == uuid.Nil {
		return fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	if idx, ok := m.indexBySystemUser[userID]; ok {
		rule := m.systemRules[idx]
		if hasSystemRole(rule.Roles, domainaccess.SystemRoleSuperuser) && m.superuserCountExcluding(userID) == 0 {
			return ErrLastSuperuser
		}
	}
	oldSystemRules := append([]domainaccess.SystemAccessRule(nil), m.systemRules...)
	oldSpaceRules := append([]domainaccess.SpaceAccessRule(nil), m.spaceRules...)
	newSystemRules := make([]domainaccess.SystemAccessRule, 0, len(m.systemRules))
	for _, rule := range m.systemRules {
		if rule.UserID != userID {
			newSystemRules = append(newSystemRules, rule)
		}
	}
	newSpaceRules := make([]domainaccess.SpaceAccessRule, 0, len(m.spaceRules))
	for _, rule := range m.spaceRules {
		if rule.UserID != userID {
			newSpaceRules = append(newSpaceRules, rule)
		}
	}
	m.systemRules = newSystemRules
	m.spaceRules = newSpaceRules
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.systemRules = oldSystemRules
		m.spaceRules = oldSpaceRules
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	oldRules := append([]domainaccess.SpaceAccessRule(nil), m.spaceRules...)
	newRules := make([]domainaccess.SpaceAccessRule, 0, len(m.spaceRules))
	for _, rule := range m.spaceRules {
		if rule.SpaceID != spaceID {
			newRules = append(newRules, rule)
		}
	}
	m.spaceRules = newRules
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.spaceRules = oldRules
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) Can(ctx context.Context, userID identity.UserID, spaceID domainspace.SpaceID, permission domainaccess.SpacePermission) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if userID == uuid.Nil {
		return false, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	if spaceID == uuid.Nil {
		return false, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if !validPermission(permission) {
		return false, fmt.Errorf("%w: invalid permission", ErrInvalidInput)
	}
	idx, ok := m.indexBySpaceUser[spaceUserKey(spaceID, userID)]
	if !ok {
		return false, nil
	}
	for _, granted := range m.spaceRules[idx].Permissions {
		if domainaccess.PermissionImplies(granted, permission) {
			return true, nil
		}
	}
	return false, nil
}

func (m *defaultManager) RulesForSpace(ctx context.Context, spaceID domainspace.SpaceID) ([]domainaccess.SpaceAccessRule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	out := []domainaccess.SpaceAccessRule{}
	for _, rule := range m.spaceRules {
		if rule.SpaceID == spaceID {
			out = append(out, cloneSpaceRule(rule))
		}
	}
	return out, nil
}

func (m *defaultManager) rebuildIndex() {
	m.indexBySystemUser = map[identity.UserID]int{}
	for i, rule := range m.systemRules {
		m.indexBySystemUser[rule.UserID] = i
	}
	m.indexBySpaceUser = map[string]int{}
	for i, rule := range m.spaceRules {
		m.indexBySpaceUser[spaceUserKey(rule.SpaceID, rule.UserID)] = i
	}
}

func (m *defaultManager) persist() error {
	b, err := json.MarshalIndent(accessStore{SystemRules: m.systemRules, SpaceRules: m.spaceRules}, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return filestore.WriteFileAtomic(m.storePath, b, 0o600)
}

func (m *defaultManager) superuserCountExcluding(excludedUserID identity.UserID) int {
	count := 0
	for _, rule := range m.systemRules {
		if rule.UserID == excludedUserID {
			continue
		}
		if hasSystemRole(rule.Roles, domainaccess.SystemRoleSuperuser) {
			count++
		}
	}
	return count
}

func (m *defaultManager) adminCountExcluding(spaceID domainspace.SpaceID, excludedUserID identity.UserID) int {
	count := 0
	for _, rule := range m.spaceRules {
		if rule.SpaceID != spaceID || rule.UserID == excludedUserID {
			continue
		}
		if hasPermission(rule.Permissions, domainaccess.SpacePermissionAdmin) {
			count++
		}
	}
	return count
}

func normalizeSystemRoles(roles []domainaccess.SystemRole) ([]domainaccess.SystemRole, error) {
	if len(roles) == 0 {
		return nil, fmt.Errorf("%w: roles are required", ErrInvalidInput)
	}
	seen := map[domainaccess.SystemRole]struct{}{}
	out := []domainaccess.SystemRole{}
	for _, role := range roles {
		if !validSystemRole(role) {
			return nil, fmt.Errorf("%w: invalid system role %q", ErrInvalidInput, role)
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	return out, nil
}

func normalizePermissions(permissions []domainaccess.SpacePermission) ([]domainaccess.SpacePermission, error) {
	if len(permissions) == 0 {
		return nil, fmt.Errorf("%w: permissions are required", ErrInvalidInput)
	}
	seen := map[domainaccess.SpacePermission]struct{}{}
	out := []domainaccess.SpacePermission{}
	for _, permission := range permissions {
		if !validPermission(permission) {
			return nil, fmt.Errorf("%w: invalid permission %q", ErrInvalidInput, permission)
		}
		if _, ok := seen[permission]; ok {
			continue
		}
		seen[permission] = struct{}{}
		out = append(out, permission)
	}
	return out, nil
}

func validSystemRole(role domainaccess.SystemRole) bool {
	switch role {
	case domainaccess.SystemRoleSuperuser, domainaccess.SystemRoleUserAdmin, domainaccess.SystemRoleOperator:
		return true
	default:
		return false
	}
}

func validSystemPermission(permission domainaccess.SystemPermission) bool {
	switch permission {
	case domainaccess.SystemPermissionManageUsers, domainaccess.SystemPermissionCreateSpaces, domainaccess.SystemPermissionManageAccess, domainaccess.SystemPermissionOperateSystem:
		return true
	default:
		return false
	}
}

func validPermission(permission domainaccess.SpacePermission) bool {
	switch permission {
	case domainaccess.SpacePermissionRead, domainaccess.SpacePermissionWrite, domainaccess.SpacePermissionAdmin:
		return true
	default:
		return false
	}
}

func hasSystemRole(roles []domainaccess.SystemRole, wanted domainaccess.SystemRole) bool {
	for _, role := range roles {
		if role == wanted {
			return true
		}
	}
	return false
}

func hasPermission(permissions []domainaccess.SpacePermission, wanted domainaccess.SpacePermission) bool {
	for _, permission := range permissions {
		if permission == wanted {
			return true
		}
	}
	return false
}

func cloneSystemRule(rule domainaccess.SystemAccessRule) domainaccess.SystemAccessRule {
	rule.Roles = append([]domainaccess.SystemRole(nil), rule.Roles...)
	return rule
}

func cloneSpaceRule(rule domainaccess.SpaceAccessRule) domainaccess.SpaceAccessRule {
	rule.Permissions = append([]domainaccess.SpacePermission(nil), rule.Permissions...)
	return rule
}

func spaceUserKey(spaceID domainspace.SpaceID, userID identity.UserID) string {
	return spaceID.String() + ":" + userID.String()
}
