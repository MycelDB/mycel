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

	firstHash := testRefreshTokenHash(t, "first")
	created, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), firstHash))
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
	if got.ID != created.ID || got.RefreshTokenHash != firstHash || got.UserID != created.UserID {
		t.Fatalf("unexpected reloaded session: %#v", got)
	}
}

func TestDefaultManager_FindByTokenHashAndListByUser(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	userID := identity.UserID(uuid.New())
	otherUserID := identity.UserID(uuid.New())
	firstHash := testRefreshTokenHash(t, "first")
	secondHash := testRefreshTokenHash(t, "second")
	thirdHash := testRefreshTokenHash(t, "third")
	first, err := m.Create(ctx, validRefreshSession(userID, firstHash))
	if err != nil {
		t.Fatalf("create first failed: %v", err)
	}
	if _, err := m.Create(ctx, validRefreshSession(userID, secondHash)); err != nil {
		t.Fatalf("create second failed: %v", err)
	}
	if _, err := m.Create(ctx, validRefreshSession(otherUserID, thirdHash)); err != nil {
		t.Fatalf("create third failed: %v", err)
	}

	found, err := m.FindByTokenHash(ctx, " "+firstHash+" ")
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
	duplicateHash := testRefreshTokenHash(t, "duplicate")
	if _, err := m.Create(ctx, validRefreshSession(userID, duplicateHash)); err != nil {
		t.Fatalf("create first failed: %v", err)
	}
	_, err := m.Create(ctx, validRefreshSession(userID, duplicateHash))
	if !errors.Is(err, ErrDuplicateTokenHash) {
		t.Fatalf("expected ErrDuplicateTokenHash, got %v", err)
	}
}

func TestDefaultManager_UpdateRotatesTokenHash(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	oldHash := testRefreshTokenHash(t, "old")
	newHash := testRefreshTokenHash(t, "new")
	created, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), oldHash))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	created.RefreshTokenHash = newHash
	created.RotationCounter++
	created.LastUsedAt = created.LastUsedAt.Add(time.Minute)
	updated, err := m.Update(ctx, created)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if updated.RotationCounter != 1 || updated.RefreshTokenHash != newHash {
		t.Fatalf("unexpected updated session: %#v", updated)
	}
	if _, err := m.FindByTokenHash(ctx, oldHash); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected old token hash to be removed from index, got %v", err)
	}
	found, err := m.FindByTokenHash(ctx, newHash)
	if err != nil {
		t.Fatalf("new token hash not indexed: %v", err)
	}
	if found.ID != created.ID {
		t.Fatalf("expected updated session, got %#v", found)
	}
}

func TestDefaultManager_ConsumedTokenHashLookup(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	oldHash := testRefreshTokenHash(t, "consumed-old")
	newHash := testRefreshTokenHash(t, "consumed-new")
	created, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), oldHash))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	created.RefreshTokenHash = newHash
	created.ConsumedRefreshTokenHashes = append(created.ConsumedRefreshTokenHashes, oldHash)
	if _, err := m.Update(ctx, created); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	found, err := m.FindByConsumedTokenHash(ctx, oldHash)
	if err != nil {
		t.Fatalf("find consumed hash failed: %v", err)
	}
	if found.ID != created.ID {
		t.Fatalf("expected consumed hash session %s, got %s", created.ID, found.ID)
	}
	if _, err := m.FindByConsumedTokenHash(ctx, newHash); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("active hash must not be indexed as consumed, got %v", err)
	}
}

func TestDefaultManager_RevokeFamily(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	familyID := domainauth.TokenFamilyID(uuid.NewString())
	first := validRefreshSession(identity.UserID(uuid.New()), testRefreshTokenHash(t, "family-first"))
	first.TokenFamilyID = familyID
	first, err := m.Create(ctx, first)
	if err != nil {
		t.Fatalf("create first failed: %v", err)
	}
	second := validRefreshSession(first.UserID, testRefreshTokenHash(t, "family-second"))
	second.TokenFamilyID = familyID
	second, err = m.Create(ctx, second)
	if err != nil {
		t.Fatalf("create second failed: %v", err)
	}
	other := validRefreshSession(first.UserID, testRefreshTokenHash(t, "family-other"))
	other.TokenFamilyID = domainauth.TokenFamilyID(uuid.NewString())
	other, err = m.Create(ctx, other)
	if err != nil {
		t.Fatalf("create other failed: %v", err)
	}

	revokedAt := time.Now().UTC().Add(-time.Minute)
	count, err := m.RevokeFamily(ctx, familyID, revokedAt, "reuse")
	if err != nil {
		t.Fatalf("revoke family failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 revoked sessions, got %d", count)
	}
	for _, id := range []domainauth.RefreshSessionID{first.ID, second.ID} {
		got, err := m.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("get revoked session failed: %v", err)
		}
		if got.Status != domainauth.RefreshSessionStatusRevoked || got.RevokedReason != "reuse" || !got.RevokedAt.Equal(revokedAt) {
			t.Fatalf("unexpected revoked session: %#v", got)
		}
	}
	gotOther, err := m.GetByID(ctx, other.ID)
	if err != nil {
		t.Fatalf("get other failed: %v", err)
	}
	if gotOther.Status != domainauth.RefreshSessionStatusActive {
		t.Fatalf("expected other session active, got %#v", gotOther)
	}
}

func TestDefaultManager_RevokeByID(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	created, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), testRefreshTokenHash(t, "revoke")))
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

	revokedOldHash := testRefreshTokenHash(t, "revoked-old")
	expiredOldHash := testRefreshTokenHash(t, "expired-old")
	activeRecentHash := testRefreshTokenHash(t, "active-recent")
	revokedOldConsumedHash := testRefreshTokenHash(t, "revoked-old-consumed")
	revokedOld := validRefreshSession(userID, revokedOldHash)
	revokedOld.ConsumedRefreshTokenHashes = []string{revokedOldConsumedHash}
	revokedOld.Status = domainauth.RefreshSessionStatusRevoked
	revokedOld.RevokedAt = cutoff.Add(-time.Hour)
	revokedOld.RevokedReason = "logout"
	revokedOld, err := m.Create(ctx, revokedOld)
	if err != nil {
		t.Fatalf("create revoked old failed: %v", err)
	}

	expiredOld := validRefreshSession(userID, expiredOldHash)
	expiredOld.IdleExpiresAt = cutoff.Add(-time.Hour)
	expiredOld.AbsoluteExpiresAt = cutoff.Add(-30 * time.Minute)
	expiredOld, err = m.Create(ctx, expiredOld)
	if err != nil {
		t.Fatalf("create expired old failed: %v", err)
	}

	activeRecent := validRefreshSession(userID, activeRecentHash)
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
	if _, err := m.FindByTokenHash(ctx, revokedOldHash); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected revoked-old hash to be removed from index, got %v", err)
	}
	if _, err := m.FindByConsumedTokenHash(ctx, revokedOldConsumedHash); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected revoked-old consumed hash to be removed from index, got %v", err)
	}
	if _, err := m.FindByTokenHash(ctx, expiredOldHash); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected expired-old hash to be removed from index, got %v", err)
	}
	if _, err := m.FindByTokenHash(ctx, activeRecentHash); err != nil {
		t.Fatalf("expected active-recent hash to remain indexed: %v", err)
	}

	gotRevoked, err := m.GetByID(ctx, revokedOld.ID)
	if err != nil {
		t.Fatalf("get revoked redacted failed: %v", err)
	}
	if gotRevoked.RefreshTokenHash != "" || len(gotRevoked.ConsumedRefreshTokenHashes) != 0 || gotRevoked.RedactedAt.IsZero() || gotRevoked.Status != domainauth.RefreshSessionStatusRevoked {
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

	token, err := domainauth.NewRefreshToken(32)
	if err != nil {
		t.Fatalf("new refresh token failed: %v", err)
	}
	rec = validRefreshSession(identity.UserID(uuid.New()), string(token))
	_, err = m.Create(ctx, rec)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for plaintext refresh token, got %v", err)
	}
}

func TestDefaultManager_DeleteExpiredRedactedMarksRecentExpiredWithoutRedacting(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	now := time.Now().UTC()
	cutoff := now.Add(-24 * time.Hour)
	recentExpiredHash := testRefreshTokenHash(t, "recent-expired")
	recentExpired := validRefreshSession(identity.UserID(uuid.New()), recentExpiredHash)
	recentExpired.IdleExpiresAt = now.Add(-time.Hour)
	recentExpired.AbsoluteExpiresAt = now.Add(time.Hour)
	recentExpired, err := m.Create(ctx, recentExpired)
	if err != nil {
		t.Fatalf("create recent expired failed: %v", err)
	}

	count, err := m.DeleteExpiredRedacted(ctx, cutoff)
	if err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one changed session, got %d", count)
	}
	got, err := m.GetByID(ctx, recentExpired.ID)
	if err != nil {
		t.Fatalf("get recent expired failed: %v", err)
	}
	if got.Status != domainauth.RefreshSessionStatusExpired || got.RefreshTokenHash != recentExpiredHash || !got.RedactedAt.IsZero() {
		t.Fatalf("expected recent expired session marked but not redacted, got %#v", got)
	}
	count, err = m.DeleteExpiredRedacted(ctx, cutoff)
	if err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected idempotent cleanup to change 0 sessions, got %d", count)
	}
}

func TestDefaultManager_RecordAndListAuditEvents(t *testing.T) {
	ctx := context.Background()
	m := initializedManager(t)
	userID := identity.UserID(uuid.New())
	otherUserID := identity.UserID(uuid.New())
	event, err := m.RecordAuditEvent(ctx, domainauth.AuthAuditEvent{Type: " auth.login_success ", UserID: &userID, UserRef: identity.UserRef("user@example.test"), Message: " login ok "})
	if err != nil {
		t.Fatalf("record audit event failed: %v", err)
	}
	if event.ID == uuid.Nil || event.Type != "auth.login_success" || event.Message != "login ok" || event.CreatedAt.IsZero() {
		t.Fatalf("unexpected audit event: %#v", event)
	}
	if _, err := m.RecordAuditEvent(ctx, domainauth.AuthAuditEvent{Type: "auth.login_failure", UserID: &otherUserID}); err != nil {
		t.Fatalf("record other audit event failed: %v", err)
	}
	listed, err := m.ListAuditEvents(ctx, &userID)
	if err != nil {
		t.Fatalf("list audit events failed: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != event.ID {
		t.Fatalf("expected one matching audit event, got %#v", listed)
	}
	all, err := m.ListAuditEvents(ctx, nil)
	if err != nil {
		t.Fatalf("list all audit events failed: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected two audit events, got %d", len(all))
	}
}

func TestDefaultManager_PersistsHashWithoutPlaintextRefreshToken(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "store")
	m := NewManager()
	if err := m.Init(ctx, dir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	token, err := domainauth.NewRefreshToken(32)
	if err != nil {
		t.Fatalf("new refresh token failed: %v", err)
	}
	hash, err := domainauth.HashRefreshToken(token)
	if err != nil {
		t.Fatalf("hash refresh token failed: %v", err)
	}
	if _, err := m.Create(ctx, validRefreshSession(identity.UserID(uuid.New()), hash)); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, refreshSessionsStoreFile))
	if err != nil {
		t.Fatalf("read store failed: %v", err)
	}
	if strings.Contains(string(raw), string(token)) {
		t.Fatalf("store must not contain plaintext refresh token")
	}
	if !strings.Contains(string(raw), hash) {
		t.Fatalf("store should contain refresh token hash")
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

func testRefreshTokenHash(t *testing.T, label string) string {
	t.Helper()
	hash, err := domainauth.HashRefreshToken(domainauth.RefreshToken("test-refresh-token-" + label))
	if err != nil {
		t.Fatalf("hash refresh token failed: %v", err)
	}
	return hash
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
