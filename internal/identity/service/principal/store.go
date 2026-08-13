package principal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const StoreFilename = "store.json"

type Store interface {
	ListPrincipals(context.Context) ([]Principal, error)
	GetPrincipal(ctx context.Context, principalID string) (Principal, error)
	FindPrincipal(ctx context.Context, username string, email string) (Principal, error)
	ApplyPrincipalPut(ctx context.Context, principal Principal) (Principal, error)
	ListRoleBindings(ctx context.Context, principalID string) ([]RoleBinding, error)
	ApplyRoleBindingPut(ctx context.Context, binding RoleBinding) (RoleBinding, error)
	ListCapabilityGrants(ctx context.Context, principalID string) ([]CapabilityGrant, error)
	ApplyCapabilityGrantPut(ctx context.Context, grant CapabilityGrant) (CapabilityGrant, error)
}

type FileStore struct {
	path string
	mu   sync.Mutex
}

type storeDocument struct {
	Principals       []Principal       `json:"principals"`
	RoleBindings     []RoleBinding     `json:"role_bindings"`
	CapabilityGrants []CapabilityGrant `json:"capability_grants"`
}

func OpenStore(dir string) (*FileStore, bool, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, false, err
	}
	path := filepath.Join(dir, StoreFilename)
	if _, err := os.Stat(path); err == nil {
		return &FileStore{path: path}, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("stat principal store: %w", err)
	}
	store := &FileStore{path: path}
	if err := store.write(storeDocument{Principals: []Principal{}, RoleBindings: []RoleBinding{}, CapabilityGrants: []CapabilityGrant{}}); err != nil {
		return nil, false, err
	}
	return store, true, nil
}

func OpenExistingStore(dir string) (*FileStore, error) {
	path := filepath.Join(dir, StoreFilename)
	if _, err := os.Stat(path); err == nil {
		return &FileStore{path: path}, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return nil, ErrPrincipalNotFound
	} else {
		return nil, fmt.Errorf("stat principal store: %w", err)
	}
}

func (s *FileStore) ListPrincipals(ctx context.Context) ([]Principal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return nil, err
	}
	out := make([]Principal, 0, len(doc.Principals))
	for _, principal := range doc.Principals {
		out = append(out, principal.normalized())
	}
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Username) < strings.ToLower(out[j].Username) })
	return out, nil
}

func (s *FileStore) GetPrincipal(ctx context.Context, principalID string) (Principal, error) {
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	principalID = strings.TrimSpace(principalID)
	if principalID == "" {
		return Principal{}, ErrPrincipalNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return Principal{}, err
	}
	for _, principal := range doc.Principals {
		if principal.ID == principalID {
			return principal.normalized(), nil
		}
	}
	return Principal{}, ErrPrincipalNotFound
}

func (s *FileStore) FindPrincipal(ctx context.Context, username string, email string) (Principal, error) {
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	username = strings.TrimSpace(username)
	email = strings.TrimSpace(email)
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return Principal{}, err
	}
	for _, principal := range doc.Principals {
		if username != "" && strings.EqualFold(principal.Username, username) {
			return principal.normalized(), nil
		}
		if email != "" && principal.Email != "" && strings.EqualFold(principal.Email, email) {
			return principal.normalized(), nil
		}
	}
	return Principal{}, ErrPrincipalNotFound
}

func (s *FileStore) ApplyPrincipalPut(ctx context.Context, principal Principal) (Principal, error) {
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	principal = normalizePrincipal(principal)
	if principal.ID == "" || (principal.Username == "" && principal.Kind != PrincipalKindSystem) {
		return Principal{}, ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return Principal{}, err
	}
	for i, existing := range doc.Principals {
		if existing.ID == principal.ID {
			if err := ensureNoPrincipalConflict(doc, principal, principal.ID); err != nil {
				return Principal{}, err
			}
			doc.Principals[i] = principal
			return principal, s.write(doc)
		}
	}
	if err := ensureNoPrincipalConflict(doc, principal, ""); err != nil {
		return Principal{}, err
	}
	doc.Principals = append(doc.Principals, principal)
	return principal, s.write(doc)
}

func (s *FileStore) ListRoleBindings(ctx context.Context, principalID string) ([]RoleBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	principalID = strings.TrimSpace(principalID)
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return nil, err
	}
	out := []RoleBinding{}
	for _, binding := range doc.RoleBindings {
		if principalID == "" || binding.PrincipalID == principalID {
			out = append(out, normalizeRoleBinding(binding))
		}
	}
	return out, nil
}

func (s *FileStore) ApplyRoleBindingPut(ctx context.Context, binding RoleBinding) (RoleBinding, error) {
	if err := ctx.Err(); err != nil {
		return RoleBinding{}, err
	}
	binding = normalizeRoleBinding(binding)
	if binding.ID == "" || binding.PrincipalID == "" || binding.Role == "" {
		return RoleBinding{}, ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return RoleBinding{}, err
	}
	if !documentHasPrincipal(doc, binding.PrincipalID) {
		return RoleBinding{}, ErrPrincipalNotFound
	}
	for i, existing := range doc.RoleBindings {
		if existing.ID == binding.ID {
			doc.RoleBindings[i] = binding
			return binding, s.write(doc)
		}
	}
	doc.RoleBindings = append(doc.RoleBindings, binding)
	return binding, s.write(doc)
}

func (s *FileStore) ListCapabilityGrants(ctx context.Context, principalID string) ([]CapabilityGrant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	principalID = strings.TrimSpace(principalID)
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return nil, err
	}
	out := []CapabilityGrant{}
	for _, grant := range doc.CapabilityGrants {
		if principalID == "" || grant.PrincipalID == principalID {
			out = append(out, normalizeCapabilityGrant(grant))
		}
	}
	return out, nil
}

func (s *FileStore) ApplyCapabilityGrantPut(ctx context.Context, grant CapabilityGrant) (CapabilityGrant, error) {
	if err := ctx.Err(); err != nil {
		return CapabilityGrant{}, err
	}
	grant = normalizeCapabilityGrant(grant)
	if grant.ID == "" || grant.PrincipalID == "" || grant.Capability == "" {
		return CapabilityGrant{}, ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return CapabilityGrant{}, err
	}
	if !documentHasPrincipal(doc, grant.PrincipalID) {
		return CapabilityGrant{}, ErrPrincipalNotFound
	}
	for i, existing := range doc.CapabilityGrants {
		if existing.ID == grant.ID {
			doc.CapabilityGrants[i] = grant
			return grant, s.write(doc)
		}
	}
	doc.CapabilityGrants = append(doc.CapabilityGrants, grant)
	return grant, s.write(doc)
}

func (s *FileStore) read() (storeDocument, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return storeDocument{}, fmt.Errorf("read principal store: %w", err)
	}
	var doc storeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return storeDocument{}, fmt.Errorf("decode principal store: %w", err)
	}
	if doc.Principals == nil {
		doc.Principals = []Principal{}
	}
	if doc.RoleBindings == nil {
		doc.RoleBindings = []RoleBinding{}
	}
	if doc.CapabilityGrants == nil {
		doc.CapabilityGrants = []CapabilityGrant{}
	}
	for i := range doc.Principals {
		doc.Principals[i] = normalizePrincipal(doc.Principals[i])
	}
	for i := range doc.RoleBindings {
		doc.RoleBindings[i] = normalizeRoleBinding(doc.RoleBindings[i])
	}
	for i := range doc.CapabilityGrants {
		doc.CapabilityGrants[i] = normalizeCapabilityGrant(doc.CapabilityGrants[i])
	}
	return doc, nil
}

func (s *FileStore) write(doc storeDocument) error {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode principal store: %w", err)
	}
	data = append(data, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write principal store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace principal store: %w", err)
	}
	return nil
}

func ensureNoPrincipalConflict(doc storeDocument, principal Principal, sameID string) error {
	for _, existing := range doc.Principals {
		if sameID != "" && existing.ID == sameID {
			continue
		}
		if existing.ID == principal.ID || (principal.Username != "" && strings.EqualFold(existing.Username, principal.Username)) || (principal.Email != "" && existing.Email != "" && strings.EqualFold(existing.Email, principal.Email)) {
			return ErrDuplicatePrincipal
		}
	}
	return nil
}

func documentHasPrincipal(doc storeDocument, principalID string) bool {
	for _, principal := range doc.Principals {
		if principal.ID == principalID {
			return true
		}
	}
	return false
}

func normalizePrincipal(principal Principal) Principal {
	principal.ID = strings.TrimSpace(principal.ID)
	principal.Username = strings.TrimSpace(principal.Username)
	principal.Email = strings.TrimSpace(principal.Email)
	principal.DisplayName = strings.TrimSpace(principal.DisplayName)
	principal.Kind = normalizePrincipalKind(principal.Kind)
	if principal.State == "" {
		principal.State = PrincipalStateActive
	}
	principal.State = strings.TrimSpace(principal.State)
	principal.CreatedBy = strings.TrimSpace(principal.CreatedBy)
	if principal.UpdatedAt.IsZero() {
		principal.UpdatedAt = principal.CreatedAt
	}
	return principal
}

func normalizePrincipalKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case PrincipalKindService:
		return PrincipalKindService
	case PrincipalKindSystem:
		return PrincipalKindSystem
	default:
		return PrincipalKindHuman
	}
}

func normalizeRoleBinding(binding RoleBinding) RoleBinding {
	binding.ID = strings.TrimSpace(binding.ID)
	binding.PrincipalID = strings.TrimSpace(binding.PrincipalID)
	binding.Role = canonicalRole(binding.Role)
	binding.Scope = normalizeScope(binding.Scope)
	if binding.State == "" {
		binding.State = GrantStateActive
	}
	binding.State = strings.TrimSpace(binding.State)
	binding.Reason = strings.TrimSpace(binding.Reason)
	binding.CreatedBy = strings.TrimSpace(binding.CreatedBy)
	binding.RevokedBy = strings.TrimSpace(binding.RevokedBy)
	if binding.CreatedAt.IsZero() {
		binding.CreatedAt = time.Now().UTC()
	}
	return binding
}

func normalizeCapabilityGrant(grant CapabilityGrant) CapabilityGrant {
	grant.ID = strings.TrimSpace(grant.ID)
	grant.PrincipalID = strings.TrimSpace(grant.PrincipalID)
	grant.Capability = canonicalCapability(grant.Capability)
	grant.Scope = normalizeScope(grant.Scope)
	if grant.State == "" {
		grant.State = GrantStateActive
	}
	grant.State = strings.TrimSpace(grant.State)
	grant.Reason = strings.TrimSpace(grant.Reason)
	grant.CreatedBy = strings.TrimSpace(grant.CreatedBy)
	grant.RevokedBy = strings.TrimSpace(grant.RevokedBy)
	if grant.CreatedAt.IsZero() {
		grant.CreatedAt = time.Now().UTC()
	}
	return grant
}

func normalizeScope(scope AccessScope) AccessScope {
	scope.Type = strings.TrimSpace(scope.Type)
	if scope.Type == "" {
		scope.Type = "system"
	}
	scope.SpaceID = strings.TrimSpace(scope.SpaceID)
	scope.DomainID = strings.TrimSpace(scope.DomainID)
	return scope
}
