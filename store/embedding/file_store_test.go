package embedding

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
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
	mgr := NewManager()
	if err := mgr.Init(ctx, t.TempDir(), ""); err != nil {
		t.Fatal(err)
	}
	ownerA := identity.UserID(uuid.New())
	ownerB := identity.UserID(uuid.New())
	if _, err := mgr.AddProfile(ctx, AddProfileInput{OwnerID: ownerA, Name: "a", ProviderID: "openai", ModelID: "openai/text-embedding-3-small"}); err != nil {
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
	if len(profilesA) != 1 || len(profilesB) != 0 {
		t.Fatalf("unexpected scoped profiles: a=%d b=%d", len(profilesA), len(profilesB))
	}
}
