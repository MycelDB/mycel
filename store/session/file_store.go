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
	domainauth "github.com/myceldb/mycel/domain/auth"
	"github.com/myceldb/mycel/domain/identity"
	"github.com/myceldb/mycel/internal/filestore"
)

const refreshSessionsStoreFile = "refresh_sessions.json"

type defaultManager struct {
	location         string
	storePath        string
	sessions         []domainauth.RefreshSession
	indexByID        map[domainauth.RefreshSessionID]int
	indexByTokenHash map[string]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexByID: map[domainauth.RefreshSessionID]int{}, indexByTokenHash: map[string]int{}}
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
			m.rebuildIndex()
			return m.persist()
		}
		return err
	}

	raw, err := os.ReadFile(m.storePath)
	if err != nil {
		return err
	}
	var sessions []domainauth.RefreshSession
	if err := json.Unmarshal(raw, &sessions); err != nil {
		return err
	}
	m.sessions = normalizeSessions(sessions)
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

func (m *defaultManager) ListByUser(ctx context.Context, userID identity.UserID) ([]domainauth.RefreshSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if userID == uuid.Nil {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	out := []domainauth.RefreshSession{}
	for _, rec := range m.sessions {
		if rec.UserID == userID {
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
		if m.sessions[i].RefreshTokenHash == "" {
			continue
		}
		if shouldRedact(m.sessions[i], cutoff) {
			if m.sessions[i].Status == domainauth.RefreshSessionStatusActive {
				m.sessions[i].Status = domainauth.RefreshSessionStatusExpired
			}
			m.sessions[i].RefreshTokenHash = ""
			m.sessions[i].RedactedAt = now
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

func (m *defaultManager) rebuildIndex() {
	m.indexByID = map[domainauth.RefreshSessionID]int{}
	m.indexByTokenHash = map[string]int{}
	for i, rec := range m.sessions {
		m.indexByID[rec.ID] = i
		if rec.RefreshTokenHash != "" {
			m.indexByTokenHash[rec.RefreshTokenHash] = i
		}
	}
}

func (m *defaultManager) persist() error {
	b, err := json.MarshalIndent(m.sessions, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return filestore.WriteFileAtomic(m.storePath, b, 0o600)
}

func validateSession(rec domainauth.RefreshSession, creating bool) error {
	if rec.UserID == uuid.Nil {
		return fmt.Errorf("%w: user_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(string(rec.UserRef)) == "" {
		return fmt.Errorf("%w: user_ref is required", ErrInvalidInput)
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
	rec.UserRef = identity.UserRef(strings.TrimSpace(string(rec.UserRef)))
	rec.TokenFamilyID = domainauth.TokenFamilyID(strings.TrimSpace(string(rec.TokenFamilyID)))
	rec.RefreshTokenHash = strings.TrimSpace(rec.RefreshTokenHash)
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
