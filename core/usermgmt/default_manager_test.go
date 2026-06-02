package usermgmt

import (
	"context"
	"encoding/base64"
	"errors"
	"path/filepath"
	"testing"

	"knot_db/model"
)

func TestDefaultUserManager_InitAndCreate_Plaintext(t *testing.T) {
	m := NewUserManager()
	dir := filepath.Join(t.TempDir(), "store")
	if err := m.Init(context.Background(), dir, ""); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	status := model.UserStatusActive
	_, err := m.Create(context.Background(), CreateUserInput{
		User:     model.UserInput{Ref: model.UserRef("Admin@Example.com"), Status: status},
		Password: "secret",
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	exists, err := m.ExistsByRef(context.Background(), model.UserRef("admin@example.com"))
	if err != nil || !exists {
		t.Fatalf("expected case-insensitive exists, got exists=%v err=%v", exists, err)
	}
}

func TestDefaultUserManager_Init_WithWrongKeyFailsDecrypt(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	keyA := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	keyB := base64.StdEncoding.EncodeToString([]byte("abcdef0123456789abcdef0123456789"))

	m1 := NewUserManager()
	if err := m1.Init(context.Background(), dir, keyA); err != nil {
		t.Fatalf("init with keyA failed: %v", err)
	}
	status := model.UserStatusActive
	_, err := m1.Create(context.Background(), CreateUserInput{
		User:     model.UserInput{Ref: model.UserRef("admin@example.com"), Status: status},
		Password: "secret",
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	m2 := NewUserManager()
	err = m2.Init(context.Background(), dir, keyB)
	if err == nil {
		t.Fatal("expected decrypt failure with wrong key")
	}
	if !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("expected ErrDecryptFailed, got: %v", err)
	}
}
