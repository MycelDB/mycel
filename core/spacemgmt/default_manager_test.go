package spacemgmt

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"knot_db/model"
)

func TestDefaultSpaceManager_InitAndCreate(t *testing.T) {
	m := NewSpaceManager()
	dir := filepath.Join(t.TempDir(), "store")
	if err := m.Init(context.Background(), dir); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	ownerID := model.UserID(uuid.New())
	space, err := m.Create(context.Background(), CreateSpaceInput{OwnerID: ownerID, Name: "default"})
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

func TestDefaultSpaceManager_CreateIsIdempotentByOwnerAndName(t *testing.T) {
	m := NewSpaceManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	ownerID := model.UserID(uuid.New())
	first, err := m.Create(context.Background(), CreateSpaceInput{OwnerID: ownerID, Name: "default"})
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	second, err := m.Create(context.Background(), CreateSpaceInput{OwnerID: ownerID, Name: "default"})
	if err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	if second.SpaceID != first.SpaceID {
		t.Fatalf("expected idempotent create, got first=%s second=%s", first.SpaceID, second.SpaceID)
	}
}

func TestDefaultSpaceManager_GetByID_NotFound(t *testing.T) {
	m := NewSpaceManager()
	if err := m.Init(context.Background(), filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	_, err := m.GetByID(context.Background(), model.SpaceID(uuid.New()))
	if !errors.Is(err, ErrSpaceNotFound) {
		t.Fatalf("expected ErrSpaceNotFound, got: %v", err)
	}
}
