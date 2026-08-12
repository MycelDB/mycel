package spaces

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/identity/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestDefaultManager_InitAndCreate(t *testing.T) {
	m := NewManager()
	dir := filepath.Join(t.TempDir(), "store")
	if err := m.Init(context.Background(), dir); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	ownerID := identity.PrincipalID(uuid.NewString())
	space, err := m.Create(context.Background(), CreateInput{OwnerID: ownerID, Name: "default"})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if space.SpaceID == uuid.Nil || space.OwnerID != ownerID || space.Name != "default" || space.Status != "active" {
		t.Fatalf("unexpected space: %#v", space)
	}
	if _, err := os.Stat(filepath.Join(dir, spacesStoreFile)); err != nil {
		t.Fatalf("expected spaces store to exist: %v", err)
	}

	exists, err := m.ExistsByID(context.Background(), space.SpaceID)
	if err != nil || !exists {
		t.Fatalf("expected exists, got exists=%v err=%v", exists, err)
	}
}

func TestDefaultManager_CreateIsIdempotentByOwnerAndName(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	ownerID := identity.PrincipalID(uuid.NewString())
	first, err := m.Create(context.Background(), CreateInput{OwnerID: ownerID, Name: "default"})
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	second, err := m.Create(context.Background(), CreateInput{OwnerID: ownerID, Name: "default"})
	if err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	if second.SpaceID != first.SpaceID {
		t.Fatalf("expected idempotent create, got first=%s second=%s", first.SpaceID, second.SpaceID)
	}
}

func TestDefaultManager_GetByID_NotFound(t *testing.T) {
	m := NewManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	_, err := m.GetByID(context.Background(), domainspace.SpaceID(uuid.New()))
	if !errors.Is(err, ErrSpaceNotFound) {
		t.Fatalf("expected ErrSpaceNotFound, got: %v", err)
	}
}
