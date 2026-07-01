package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	domainauth "github.com/myceldb/mycel/domain/auth"
	"github.com/myceldb/mycel/domain/identity"
)

func TestDefaultManager_InitCreateAndReload(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "store")
	m := NewManager()
	if err := m.Init(ctx, dir); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	created, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), "sha256:first"))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if created.ID == uuid.Nil {
		t.Fatalf("expected generated session id")
	}
	if created.Status != domainauth.RefreshSessionStatusActive {
		t.Fatalf("expected active status, got %q", created.Status)
	}
	if strings.TrimSpace(string(created.TokenFamilyID)) == "" {
		t.Fatalf("expected generated token family id")
	}
	if _, err := os.Stat(filepath.Join(dir, refreshSessionsStoreFile)); err != nil {
		t.Fatalf("expected refresh session store to exist: %v", err)
	}

	reloaded := NewManager()
	if err := reloaded.Init(ctx, dir); err != nil {
		t.Fatalf("reload init failed: %v", err)
	}
	got, err := reloaded.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("get after reload failed: %v", err)
	}
	if got.ID != created.ID || got.RefreshTokenHash != "sha256:first" || got.UserID != created.UserID {
		t.Fatalf("unexpected reloaded session: %#v", got)
	}
}

func TestDefaultManager_FindByTokenHashAndListByUser(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	userID := identity.UserID(uuid.New())
	otherUserID := identity.UserID(uuid.New())
	first, err := m.Create(ctx, validRefreshSession(userID, "sha256:first"))
	if err != nil {
		t.Fatalf("create first failed: %v", err)
	}
	if _, err := m.Create(ctx, validRefreshSession(userID, "sha256:second")); err != nil {
		t.Fatalf("create second failed: %v", err)
	}
	if _, err := m.Create(ctx, validRefreshSession(otherUserID, "sha256:third")); err != nil {
		t.Fatalf("create third failed: %v", err)
	}

	found, err := m.FindByTokenHash(ctx, " sha256:first ")
	if err != nil {
		t.Fatalf("find by token hash failed: %v", err)
	}
	if found.ID != first.ID {
		t.Fatalf("expected first session, got %#v", found)
	}
	listed, err := m.ListByUser(ctx, userID)
	if err != nil {
		t.Fatalf("list by user failed: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 sessions for user, got %d", len(listed))
	}
}

func TestDefaultManager_DuplicateTokenHashRejected(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	userID := identity.UserID(uuid.New())
	if _, err := m.Create(ctx, validRefreshSession(userID, "sha256:duplicate")); err != nil {
		t.Fatalf("create first failed: %v", err)
	}
	_, err := m.Create(ctx, validRefreshSession(userID, "sha256:duplicate"))
	if !errors.Is(err, ErrDuplicateTokenHash) {
		t.Fatalf("expected ErrDuplicateTokenHash, got %v", err)
	}
}

func TestDefaultManager_UpdateRotatesTokenHash(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	created, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), "sha256:old"))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	created.RefreshTokenHash = "sha256:new"
	created.RotationCounter++
	created.LastUsedAt = created.LastUsedAt.Add(time.Minute)
	updated, err := m.Update(ctx, created)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if updated.RotationCounter != 1 || updated.RefreshTokenHash != "sha256:new" {
		t.Fatalf("unexpected updated session: %#v", updated)
	}
	if _, err := m.FindByTokenHash(ctx, "sha256:old"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected old token hash to be removed from index, got %v", err)
	}
	found, err := m.FindByTokenHash(ctx, "sha256:new")
	if err != nil {
		t.Fatalf("new token hash not indexed: %v", err)
	}
	if found.ID != created.ID {
		t.Fatalf("expected updated session, got %#v", found)
	}
}

func TestDefaultManager_RevokeByID(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	created, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), "sha256:revoke"))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	revokedAt := time.Now().UTC().Add(-time.Hour)
	revoked, err := m.RevokeByID(ctx, created.ID, revokedAt, "logout")
	if err != nil {
		t.Fatalf("revoke failed: %v", err)
	}
	if revoked.Status != domainauth.RefreshSessionStatusRevoked || !revoked.RevokedAt.Equal(revokedAt) || revoked.RevokedReason != "logout" {
		t.Fatalf("unexpected revoked session: %#v", revoked)
	}
	got, err := m.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("get revoked failed: %v", err)
	}
	if got.Status != domainauth.RefreshSessionStatusRevoked {
		t.Fatalf("expected persisted revoked status, got %#v", got)
	}
}

func TestDefaultManager_DeleteExpiredRedacted(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	userID := identity.UserID(uuid.New())
	now := time.Now().UTC()
	cutoff := now.Add(-24 * time.Hour)

	revokedOld := validRefreshSession(userID, "sha256:revoked-old")
	revokedOld.Status = domainauth.RefreshSessionStatusRevoked
	revokedOld.RevokedAt = cutoff.Add(-time.Hour)
	revokedOld.RevokedReason = "logout"
	revokedOld, err := m.Create(ctx, revokedOld)
	if err != nil {
		t.Fatalf("create revoked old failed: %v", err)
	}

	expiredOld := validRefreshSession(userID, "sha256:expired-old")
	expiredOld.IdleExpiresAt = cutoff.Add(-time.Hour)
	expiredOld.AbsoluteExpiresAt = cutoff.Add(-30 * time.Minute)
	expiredOld, err = m.Create(ctx, expiredOld)
	if err != nil {
		t.Fatalf("create expired old failed: %v", err)
	}

	activeRecent := validRefreshSession(userID, "sha256:active-recent")
	activeRecent.IdleExpiresAt = now.Add(time.Hour)
	activeRecent.AbsoluteExpiresAt = now.Add(2 * time.Hour)
	activeRecent, err = m.Create(ctx, activeRecent)
	if err != nil {
		t.Fatalf("create active recent failed: %v", err)
	}

	count, err := m.DeleteExpiredRedacted(ctx, cutoff)
	if err != nil {
		t.Fatalf("redact failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 redacted sessions, got %d", count)
	}
	if _, err := m.FindByTokenHash(ctx, "sha256:revoked-old"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected revoked-old hash to be removed from index, got %v", err)
	}
	if _, err := m.FindByTokenHash(ctx, "sha256:expired-old"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected expired-old hash to be removed from index, got %v", err)
	}
	if _, err := m.FindByTokenHash(ctx, "sha256:active-recent"); err != nil {
		t.Fatalf("expected active-recent hash to remain indexed: %v", err)
	}

	gotRevoked, err := m.GetByID(ctx, revokedOld.ID)
	if err != nil {
		t.Fatalf("get revoked redacted failed: %v", err)
	}
	if gotRevoked.RefreshTokenHash != "" || gotRevoked.RedactedAt.IsZero() || gotRevoked.Status != domainauth.RefreshSessionStatusRevoked {
		t.Fatalf("unexpected revoked redaction: %#v", gotRevoked)
	}
	gotExpired, err := m.GetByID(ctx, expiredOld.ID)
	if err != nil {
		t.Fatalf("get expired redacted failed: %v", err)
	}
	if gotExpired.RefreshTokenHash != "" || gotExpired.RedactedAt.IsZero() || gotExpired.Status != domainauth.RefreshSessionStatusExpired {
		t.Fatalf("unexpected expired redaction: %#v", gotExpired)
	}
	gotActive, err := m.GetByID(ctx, activeRecent.ID)
	if err != nil {
		t.Fatalf("get active failed: %v", err)
	}
	if gotActive.RefreshTokenHash == "" || !gotActive.RedactedAt.IsZero() || gotActive.Status != domainauth.RefreshSessionStatusActive {
		t.Fatalf("unexpected active session after redaction: %#v", gotActive)
	}
}

func TestDefaultManager_ValidationRejectsPlainMissingTokenHash(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	rec := validRefreshSession(identity.UserID(uuid.New()), "")
	_, err := m.Create(ctx, rec)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for missing refresh token hash, got %v", err)
	}
}

func initializedManager(t *testing.T) Manager {
	t.Helper()
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	return m
}

func validRefreshSession(userID identity.UserID, tokenHash string) domainauth.RefreshSession {
	now := time.Now().UTC().Truncate(time.Second)
	return domainauth.RefreshSession{
		UserID:            userID,
		UserRef:           identity.UserRef(userID.String() + "@example.test"),
		RefreshTokenHash:  tokenHash,
		CreatedAt:         now,
		LastUsedAt:        now,
		IdleExpiresAt:     now.Add(24 * time.Hour),
		AbsoluteExpiresAt: now.Add(48 * time.Hour),
		Metadata: domainauth.RefreshSessionMetadata{
			UserAgentHash: "sha256:user-agent",
			IPPrefixHash:  "sha256:ip-prefix",
			ClientName:    "test-client",
		},
	}
}
