package access

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

const accessStoreFile = "access.json"

type accessStore struct {
	Rules []model.SpaceAccessRule `json:"rules"`
}

type defaultManager struct {
	location         string
	storePath        string
	rules            []model.SpaceAccessRule
	indexBySpaceUser map[string]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexBySpaceUser: map[string]int{}}
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
			m.rules = []model.SpaceAccessRule{}
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
	m.rules = store.Rules
	m.rebuildIndex()
	return nil
}

func (m *defaultManager) Grant(ctx context.Context, in GrantInput) (model.SpaceAccessRule, error) {
	if err := ctx.Err(); err != nil {
		return model.SpaceAccessRule{}, err
	}
	if in.SpaceID == uuid.Nil {
		return model.SpaceAccessRule{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if in.UserID == uuid.Nil {
		return model.SpaceAccessRule{}, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	permissions, err := normalizePermissions(in.Permissions)
	if err != nil {
		return model.SpaceAccessRule{}, err
	}

	key := spaceUserKey(in.SpaceID, in.UserID)
	if idx, ok := m.indexBySpaceUser[key]; ok {
		oldRule := m.rules[idx]
		newRule := oldRule
		newRule.Permissions = permissions
		if hasPermission(oldRule.Permissions, model.SpacePermissionAdmin) && !hasPermission(newRule.Permissions, model.SpacePermissionAdmin) && m.adminCountExcluding(in.SpaceID, in.UserID) == 0 {
			return model.SpaceAccessRule{}, ErrLastAdmin
		}
		m.rules[idx] = newRule
		if err := m.persist(); err != nil {
			m.rules[idx] = oldRule
			return model.SpaceAccessRule{}, err
		}
		return newRule, nil
	}

	rule := model.SpaceAccessRule{ID: uuid.New(), SpaceID: in.SpaceID, UserID: in.UserID, Permissions: permissions}
	m.rules = append(m.rules, rule)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.rules = m.rules[:len(m.rules)-1]
		m.rebuildIndex()
		return model.SpaceAccessRule{}, err
	}
	return rule, nil
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
	rule := m.rules[idx]
	if hasPermission(rule.Permissions, model.SpacePermissionAdmin) && m.adminCountExcluding(in.SpaceID, in.UserID) == 0 {
		return ErrLastAdmin
	}
	oldRules := append([]model.SpaceAccessRule(nil), m.rules...)
	m.rules = append(m.rules[:idx], m.rules[idx+1:]...)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.rules = oldRules
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) Can(ctx context.Context, userID model.UserID, spaceID model.SpaceID, permission model.SpacePermission) (bool, error) {
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
	for _, granted := range m.rules[idx].Permissions {
		if model.PermissionImplies(granted, permission) {
			return true, nil
		}
	}
	return false, nil
}

func (m *defaultManager) RulesForSpace(ctx context.Context, spaceID model.SpaceID) ([]model.SpaceAccessRule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	out := []model.SpaceAccessRule{}
	for _, rule := range m.rules {
		if rule.SpaceID == spaceID {
			out = append(out, cloneRule(rule))
		}
	}
	return out, nil
}

func (m *defaultManager) rebuildIndex() {
	m.indexBySpaceUser = map[string]int{}
	for i, rule := range m.rules {
		m.indexBySpaceUser[spaceUserKey(rule.SpaceID, rule.UserID)] = i
	}
}

func (m *defaultManager) persist() error {
	b, err := json.MarshalIndent(accessStore{Rules: m.rules}, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(m.storePath, b, 0o600)
}

func (m *defaultManager) adminCountExcluding(spaceID model.SpaceID, excludedUserID model.UserID) int {
	count := 0
	for _, rule := range m.rules {
		if rule.SpaceID != spaceID || rule.UserID == excludedUserID {
			continue
		}
		if hasPermission(rule.Permissions, model.SpacePermissionAdmin) {
			count++
		}
	}
	return count
}

func normalizePermissions(permissions []model.SpacePermission) ([]model.SpacePermission, error) {
	if len(permissions) == 0 {
		return nil, fmt.Errorf("%w: permissions are required", ErrInvalidInput)
	}
	seen := map[model.SpacePermission]struct{}{}
	out := []model.SpacePermission{}
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

func validPermission(permission model.SpacePermission) bool {
	switch permission {
	case model.SpacePermissionRead, model.SpacePermissionWrite, model.SpacePermissionAdmin:
		return true
	default:
		return false
	}
}

func hasPermission(permissions []model.SpacePermission, wanted model.SpacePermission) bool {
	for _, permission := range permissions {
		if permission == wanted {
			return true
		}
	}
	return false
}

func cloneRule(rule model.SpaceAccessRule) model.SpaceAccessRule {
	rule.Permissions = append([]model.SpacePermission(nil), rule.Permissions...)
	return rule
}

func spaceUserKey(spaceID model.SpaceID, userID model.UserID) string {
	return spaceID.String() + ":" + userID.String()
}
