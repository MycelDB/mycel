package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/filestore"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/identity/model"
)

const refreshSessionsStoreFile = "refresh_sessions.json"

type storedState struct {
	RefreshSessions []domainauth.RefreshSession `json:"refresh_sessions"`
	AuditEvents     []domainauth.AuthAuditEvent `json:"audit_events,omitempty"`
}

type defaultManager struct {
	location                 string
	storePath                string
	sessions                 []domainauth.RefreshSession
	auditEvents              []domainauth.AuthAuditEvent
	indexByID                map[domainauth.RefreshSessionID]int
	indexByTokenHash         map[string]int
	indexByConsumedTokenHash map[string]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexByID: map[domainauth.RefreshSessionID]int{}, indexByTokenHash: map[string]int{}, indexByConsumedTokenHash: map[string]int{}}
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
	m.storePath = filepath.Join(location, refreshSessionsStoreFile)

	if _, err := os.Stat(m.storePath); err != nil {
		if os.IsNotExist(err) {
			m.sessions = []domainauth.RefreshSession{}
			m.auditEvents = []domainauth.AuthAuditEvent{}
			m.rebuildIndex()
			return m.persist()
		}
		return err
	}

	raw, err := os.ReadFile(m.storePath)
	if err != nil {
		return err
	}
	var state storedState
	if err := json.Unmarshal(raw, &state); err != nil || state.RefreshSessions == nil {
		// Older development snapshots stored the refresh-session array directly.
		var sessions []domainauth.RefreshSession
		if arrErr := json.Unmarshal(raw, &sessions); arrErr != nil {
			if err != nil {
				return err
			}
			return arrErr
		}
		state.RefreshSessions = sessions
	}
	m.sessions = normalizeSessions(state.RefreshSessions)
	m.auditEvents = state.AuditEvents
	if m.auditEvents == nil {
		m.auditEvents = []domainauth.AuthAuditEvent{}
	}
	m.rebuildIndex()
	return nil
}

func (m *defaultManager) Create(ctx context.Context, rec domainauth.RefreshSession) (domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return domainauth.RefreshSession{}, err
	}
	rec = normalizeSession(rec)
	if rec.ID == uuid.Nil {
		rec.ID = uuid.New()
	}
	if strings.TrimSpace(string(rec.TokenFamilyID)) == "" {
		rec.TokenFamilyID = domainauth.TokenFamilyID(uuid.NewString())
	}
	if rec.Status == "" {
		rec.Status = domainauth.RefreshSessionStatusActive
	}
	now := time.Now().UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	if rec.LastUsedAt.IsZero() {
		rec.LastUsedAt = rec.CreatedAt
	}
	if err := validateSession(rec, true); err != nil {
		return domainauth.RefreshSession{}, err
	}
	if _, exists := m.indexByID[rec.ID]; exists {
		return domainauth.RefreshSession{}, ErrDuplicateSessionID
	}
	if _, exists := m.indexByTokenHash[rec.RefreshTokenHash]; exists {
		return domainauth.RefreshSession{}, ErrDuplicateTokenHash
	}

	m.sessions = append(m.sessions, rec)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.sessions = m.sessions[:len(m.sessions)-1]
		m.rebuildIndex()
		return domainauth.RefreshSession{}, err
	}
	return rec, nil
}

func (m *defaultManager) GetByID(ctx context.Context, id domainauth.RefreshSessionID) (domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return domainauth.RefreshSession{}, err
	}
	if id == uuid.Nil {
		return domainauth.RefreshSession{}, fmt.Errorf("%w: session_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return domainauth.RefreshSession{}, ErrSessionNotFound
	}
	return m.sessions[idx], nil
}

func (m *defaultManager) FindByTokenHash(ctx context.Context, hash string) (domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return domainauth.RefreshSession{}, err
	}
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return domainauth.RefreshSession{}, fmt.Errorf("%w: refresh token hash is required", ErrInvalidInput)
	}
	idx, ok := m.indexByTokenHash[hash]
	if !ok {
		return domainauth.RefreshSession{}, ErrSessionNotFound
	}
	return m.sessions[idx], nil
}

func (m *defaultManager) FindByConsumedTokenHash(ctx context.Context, hash string) (domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return domainauth.RefreshSession{}, err
	}
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return domainauth.RefreshSession{}, fmt.Errorf("%w: refresh token hash is required", ErrInvalidInput)
	}
	idx, ok := m.indexByConsumedTokenHash[hash]
	if !ok {
		return domainauth.RefreshSession{}, ErrSessionNotFound
	}
	return m.sessions[idx], nil
}

func (m *defaultManager) ListByPrincipal(ctx context.Context, principalID identity.PrincipalID) ([]domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	principalID = identity.PrincipalID(strings.TrimSpace(string(principalID)))
	if principalID == "" {
		return nil, fmt.Errorf("%w: principal_id is required", ErrInvalidInput)
	}
	out := []domainauth.RefreshSession{}
	for _, rec := range m.sessions {
		if rec.PrincipalID == principalID {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (m *defaultManager) Update(ctx context.Context, rec domainauth.RefreshSession) (domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return domainauth.RefreshSession{}, err
	}
	rec = normalizeSession(rec)
	if rec.ID == uuid.Nil {
		return domainauth.RefreshSession{}, fmt.Errorf("%w: session_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[rec.ID]
	if !ok {
		return domainauth.RefreshSession{}, ErrSessionNotFound
	}
	if err := validateSession(rec, false); err != nil {
		return domainauth.RefreshSession{}, err
	}
	if rec.RefreshTokenHash != "" {
		if existingIdx, exists := m.indexByTokenHash[rec.RefreshTokenHash]; exists && existingIdx != idx {
			return domainauth.RefreshSession{}, ErrDuplicateTokenHash
		}
		if existingIdx, exists := m.indexByConsumedTokenHash[rec.RefreshTokenHash]; exists && existingIdx != idx {
			return domainauth.RefreshSession{}, ErrDuplicateTokenHash
		}
	}
	for _, hash := range rec.ConsumedRefreshTokenHashes {
		if existingIdx, exists := m.indexByTokenHash[hash]; exists && existingIdx != idx {
			return domainauth.RefreshSession{}, ErrDuplicateTokenHash
		}
	}

	old := m.sessions[idx]
	m.sessions[idx] = rec
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.sessions[idx] = old
		m.rebuildIndex()
		return domainauth.RefreshSession{}, err
	}
	return rec, nil
}

func (m *defaultManager) RevokeByID(ctx context.Context, id domainauth.RefreshSessionID, revokedAt time.Time, reason string) (domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return domainauth.RefreshSession{}, err
	}
	if id == uuid.Nil {
		return domainauth.RefreshSession{}, fmt.Errorf("%w: session_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return domainauth.RefreshSession{}, ErrSessionNotFound
	}
	if revokedAt.IsZero() {
		revokedAt = time.Now().UTC()
	} else {
		revokedAt = revokedAt.UTC()
	}
	rec := m.sessions[idx]
	rec.Status = domainauth.RefreshSessionStatusRevoked
	rec.RevokedAt = revokedAt
	rec.RevokedReason = strings.TrimSpace(reason)
	old := m.sessions[idx]
	m.sessions[idx] = rec
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.sessions[idx] = old
		m.rebuildIndex()
		return domainauth.RefreshSession{}, err
	}
	return rec, nil
}

func (m *defaultManager) RevokeFamily(ctx context.Context, familyID domainauth.TokenFamilyID, revokedAt time.Time, reason string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	familyID = domainauth.TokenFamilyID(strings.TrimSpace(string(familyID)))
	if familyID == "" {
		return 0, fmt.Errorf("%w: token_family_id is required", ErrInvalidInput)
	}
	if revokedAt.IsZero() {
		revokedAt = time.Now().UTC()
	} else {
		revokedAt = revokedAt.UTC()
	}
	oldSessions := append([]domainauth.RefreshSession(nil), m.sessions...)
	changed := 0
	for i := range m.sessions {
		if m.sessions[i].TokenFamilyID != familyID || m.sessions[i].Status == domainauth.RefreshSessionStatusRevoked {
			continue
		}
		m.sessions[i].Status = domainauth.RefreshSessionStatusRevoked
		m.sessions[i].RevokedAt = revokedAt
		m.sessions[i].RevokedReason = strings.TrimSpace(reason)
		changed++
	}
	if changed == 0 {
		return 0, nil
	}
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.sessions = oldSessions
		m.rebuildIndex()
		return 0, err
	}
	return changed, nil
}

func (m *defaultManager) DeleteExpiredRedacted(ctx context.Context, cutoff time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if cutoff.IsZero() {
		return 0, fmt.Errorf("%w: cutoff is required", ErrInvalidInput)
	}
	cutoff = cutoff.UTC()
	now := time.Now().UTC()
	changed := 0
	oldSessions := append([]domainauth.RefreshSession(nil), m.sessions...)
	for i := range m.sessions {
		sessionChanged := false
		if m.sessions[i].Status == domainauth.RefreshSessionStatusActive && sessionExpired(m.sessions[i], now) {
			m.sessions[i].Status = domainauth.RefreshSessionStatusExpired
			sessionChanged = true
		}
		if shouldRedact(m.sessions[i], cutoff) && (m.sessions[i].RefreshTokenHash != "" || len(m.sessions[i].ConsumedRefreshTokenHashes) > 0) {
			m.sessions[i].RefreshTokenHash = ""
			m.sessions[i].ConsumedRefreshTokenHashes = nil
			m.sessions[i].RedactedAt = now
			sessionChanged = true
		}
		if sessionChanged {
			changed++
		}
	}
	if changed == 0 {
		return 0, nil
	}
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.sessions = oldSessions
		m.rebuildIndex()
		return 0, err
	}
	return changed, nil
}

func (m *defaultManager) RecordAuditEvent(ctx context.Context, event domainauth.AuthAuditEvent) (domainauth.AuthAuditEvent, error) {
	if err := ctx.Err(); err != nil {
		return domainauth.AuthAuditEvent{}, err
	}
	event.Type = strings.TrimSpace(event.Type)
	if event.Type == "" {
		return domainauth.AuthAuditEvent{}, fmt.Errorf("%w: audit event type is required", ErrInvalidInput)
	}
	event.Message = strings.TrimSpace(event.Message)
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	} else {
		event.CreatedAt = event.CreatedAt.UTC()
	}
	oldEvents := append([]domainauth.AuthAuditEvent(nil), m.auditEvents...)
	m.auditEvents = append(m.auditEvents, event)
	if err := m.persist(); err != nil {
		m.auditEvents = oldEvents
		return domainauth.AuthAuditEvent{}, err
	}
	return event, nil
}

func (m *defaultManager) ListAuditEventsByPrincipal(ctx context.Context, principalID *identity.PrincipalID) ([]domainauth.AuthAuditEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if principalID != nil {
		trimmed := identity.PrincipalID(strings.TrimSpace(string(*principalID)))
		if trimmed == "" {
			return nil, fmt.Errorf("%w: principal_id is required", ErrInvalidInput)
		}
		principalID = &trimmed
	}
	out := []domainauth.AuthAuditEvent{}
	for _, event := range m.auditEvents {
		if principalID != nil && (event.PrincipalID == nil || *event.PrincipalID != *principalID) {
			continue
		}
		out = append(out, event)
	}
	return out, nil
}

func (m *defaultManager) rebuildIndex() {
	m.indexByID = map[domainauth.RefreshSessionID]int{}
	m.indexByTokenHash = map[string]int{}
	m.indexByConsumedTokenHash = map[string]int{}
	for i, rec := range m.sessions {
		m.indexByID[rec.ID] = i
		if rec.RefreshTokenHash != "" {
			m.indexByTokenHash[rec.RefreshTokenHash] = i
		}
		for _, hash := range rec.ConsumedRefreshTokenHashes {
			if hash != "" {
				m.indexByConsumedTokenHash[hash] = i
			}
		}
	}
}

func (m *defaultManager) persist() error {
	state := storedState{RefreshSessions: m.sessions, AuditEvents: m.auditEvents}
	if state.AuditEvents == nil {
		state.AuditEvents = []domainauth.AuthAuditEvent{}
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return filestore.WriteFileAtomic(m.storePath, b, 0o600)
}

func validateSession(rec domainauth.RefreshSession, creating bool) error {
	if strings.TrimSpace(string(rec.PrincipalID)) == "" {
		return fmt.Errorf("%w: principal_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(string(rec.PrincipalRef)) == "" {
		return fmt.Errorf("%w: principal_ref is required", ErrInvalidInput)
	}
	if strings.TrimSpace(string(rec.TokenFamilyID)) == "" {
		return fmt.Errorf("%w: token_family_id is required", ErrInvalidInput)
	}
	if rec.Status == "" {
		return fmt.Errorf("%w: status is required", ErrInvalidInput)
	}
	if rec.Status != domainauth.RefreshSessionStatusActive && rec.Status != domainauth.RefreshSessionStatusRevoked && rec.Status != domainauth.RefreshSessionStatusExpired {
		return fmt.Errorf("%w: unsupported status", ErrInvalidInput)
	}
	if rec.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created_at is required", ErrInvalidInput)
	}
	if rec.LastUsedAt.IsZero() {
		return fmt.Errorf("%w: last_used_at is required", ErrInvalidInput)
	}
	if rec.IdleExpiresAt.IsZero() {
		return fmt.Errorf("%w: idle_expires_at is required", ErrInvalidInput)
	}
	if rec.AbsoluteExpiresAt.IsZero() {
		return fmt.Errorf("%w: absolute_expires_at is required", ErrInvalidInput)
	}
	if rec.AbsoluteExpiresAt.Before(rec.IdleExpiresAt) {
		return fmt.Errorf("%w: absolute_expires_at must be after idle_expires_at", ErrInvalidInput)
	}
	if strings.TrimSpace(rec.RefreshTokenHash) == "" && (creating || rec.RedactedAt.IsZero()) {
		return fmt.Errorf("%w: refresh token hash is required", ErrInvalidInput)
	}
	if rec.RefreshTokenHash != "" && !domainauth.IsRefreshTokenHash(rec.RefreshTokenHash) {
		return fmt.Errorf("%w: refresh token hash must use a supported algorithm prefix", ErrInvalidInput)
	}
	seenConsumed := map[string]struct{}{}
	for _, hash := range rec.ConsumedRefreshTokenHashes {
		if !domainauth.IsRefreshTokenHash(hash) {
			return fmt.Errorf("%w: consumed refresh token hash must use a supported algorithm prefix", ErrInvalidInput)
		}
		if rec.RefreshTokenHash != "" && hash == rec.RefreshTokenHash {
			return fmt.Errorf("%w: consumed refresh token hash must not match active hash", ErrInvalidInput)
		}
		if _, exists := seenConsumed[hash]; exists {
			return fmt.Errorf("%w: duplicate consumed refresh token hash", ErrInvalidInput)
		}
		seenConsumed[hash] = struct{}{}
	}
	if rec.Status == domainauth.RefreshSessionStatusRevoked && rec.RevokedAt.IsZero() {
		return fmt.Errorf("%w: revoked_at is required for revoked sessions", ErrInvalidInput)
	}
	return nil
}

func normalizeSessions(in []domainauth.RefreshSession) []domainauth.RefreshSession {
	out := make([]domainauth.RefreshSession, 0, len(in))
	for _, rec := range in {
		out = append(out, normalizeSession(rec))
	}
	return out
}

func normalizeSession(rec domainauth.RefreshSession) domainauth.RefreshSession {
	rec.PrincipalID = identity.PrincipalID(strings.TrimSpace(string(rec.PrincipalID)))
	rec.PrincipalRef = identity.PrincipalRef(strings.TrimSpace(string(rec.PrincipalRef)))
	rec.TokenFamilyID = domainauth.TokenFamilyID(strings.TrimSpace(string(rec.TokenFamilyID)))
	rec.RefreshTokenHash = strings.TrimSpace(rec.RefreshTokenHash)
	if len(rec.ConsumedRefreshTokenHashes) > 0 {
		consumed := make([]string, 0, len(rec.ConsumedRefreshTokenHashes))
		seen := map[string]struct{}{}
		for _, hash := range rec.ConsumedRefreshTokenHashes {
			hash = strings.TrimSpace(hash)
			if hash == "" {
				continue
			}
			if _, exists := seen[hash]; exists {
				continue
			}
			seen[hash] = struct{}{}
			consumed = append(consumed, hash)
		}
		rec.ConsumedRefreshTokenHashes = consumed
	}
	rec.RevokedReason = strings.TrimSpace(rec.RevokedReason)
	rec.Metadata.UserAgentHash = strings.TrimSpace(rec.Metadata.UserAgentHash)
	rec.Metadata.IPPrefixHash = strings.TrimSpace(rec.Metadata.IPPrefixHash)
	rec.Metadata.ClientName = strings.TrimSpace(rec.Metadata.ClientName)
	rec.CreatedAt = rec.CreatedAt.UTC()
	rec.LastUsedAt = rec.LastUsedAt.UTC()
	rec.IdleExpiresAt = rec.IdleExpiresAt.UTC()
	rec.AbsoluteExpiresAt = rec.AbsoluteExpiresAt.UTC()
	rec.RevokedAt = rec.RevokedAt.UTC()
	rec.RedactedAt = rec.RedactedAt.UTC()
	return rec
}

func sessionExpired(rec domainauth.RefreshSession, now time.Time) bool {
	terminal := terminalExpiry(rec)
	return !terminal.IsZero() && !terminal.After(now)
}

func shouldRedact(rec domainauth.RefreshSession, cutoff time.Time) bool {
	switch rec.Status {
	case domainauth.RefreshSessionStatusRevoked:
		return !rec.RevokedAt.IsZero() && !rec.RevokedAt.After(cutoff)
	case domainauth.RefreshSessionStatusExpired:
		return terminalExpiry(rec).Before(cutoff) || terminalExpiry(rec).Equal(cutoff)
	case domainauth.RefreshSessionStatusActive:
		terminal := terminalExpiry(rec)
		return !terminal.IsZero() && !terminal.After(cutoff)
	default:
		return false
	}
}

func terminalExpiry(rec domainauth.RefreshSession) time.Time {
	if rec.IdleExpiresAt.IsZero() {
		return rec.AbsoluteExpiresAt
	}
	if rec.AbsoluteExpiresAt.IsZero() || rec.IdleExpiresAt.Before(rec.AbsoluteExpiresAt) {
		return rec.IdleExpiresAt
	}
	return rec.AbsoluteExpiresAt
}
