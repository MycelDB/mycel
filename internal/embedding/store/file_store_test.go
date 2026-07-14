package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	"github.com/myceldb/mycel/internal/identity/model"
)

func TestProviderKeySecretsAreEncryptedAndRedacted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mgr := NewManager()
	if err := mgr.Init(ctx, dir, ""); err != nil {
		t.Fatal(err)
	}
	owner := identity.UserID(uuid.New())
	key, err := mgr.AddKey(ctx, AddKeyInput{OwnerID: owner, ProviderID: "openai", Name: "personal", APIKey: "sk-secret", IsDefault: true})
	if err != nil {
		t.Fatal(err)
	}
	if !key.HasAPIKey {
		t.Fatalf("expected has_api_key")
	}
	raw, err := os.ReadFile(dir + "/embeddings.json")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sk-secret") {
		t.Fatalf("stored metadata contains plaintext secret: %s", raw)
	}
	_, secret, err := mgr.ResolveAPIKey(ctx, owner, "openai", key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if secret != "sk-secret" {
		t.Fatalf("secret mismatch: %q", secret)
	}
	listed, err := mgr.ListKeys(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || !listed[0].HasAPIKey {
		t.Fatalf("unexpected list: %#v", listed)
	}
}

func TestProfilesAreOwnerScoped(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ownerA := identity.UserID(uuid.New())
	ownerB := identity.UserID(uuid.New())
	seedLegacyProfileFixture(t, dir, ownerA)
	mgr := NewManager()
	if err := mgr.Init(ctx, dir, ""); err != nil {
		t.Fatal(err)
	}
	profilesA, err := mgr.ListProfiles(ctx, ownerA)
	if err != nil {
		t.Fatal(err)
	}
	profilesB, err := mgr.ListProfiles(ctx, ownerB)
	if err != nil {
		t.Fatal(err)
	}
	if len(profilesA) != 1 || profilesA[0].Name != "a" || len(profilesB) != 0 {
		t.Fatalf("unexpected scoped profiles: a=%#v b=%#v", profilesA, profilesB)
	}
}

func seedLegacyProfileFixture(t *testing.T, dir string, ownerID identity.UserID) {
	t.Helper()
	now := time.Now().UTC()
	profile := domainembedding.Profile{ID: uuid.New(), OwnerID: ownerID, Name: "a", ProviderID: "openai", ModelID: "openai/text-embedding-3-small", SourceMode: domainembedding.SourceModeSubtree, CreatedAt: now, UpdatedAt: now}
	raw, err := json.Marshal(storedData{Keys: []storedKey{}, Profiles: []domainembedding.Profile{profile}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, storeFile), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}
