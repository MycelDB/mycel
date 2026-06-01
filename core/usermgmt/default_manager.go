package usermgmt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"knot_db/core/identity"
)

const usersStoreFile = "users.json"

type storedUser struct {
	User     identity.User `json:"user"`
	Password string        `json:"password"`
}

type encryptedStore struct {
	Format    string `json:"format"`
	NonceB64  string `json:"nonce_b64"`
	CipherB64 string `json:"cipher_b64"`
}

// DefaultUserManager is a file-backed UserManager implementation.
type DefaultUserManager struct {
	location     string
	storePath    string
	key          []byte
	encrypted    bool
	users        []storedUser
	indexByRefLC map[string]int
}

func NewDefaultUserManager() *DefaultUserManager {
	return &DefaultUserManager{indexByRefLC: map[string]int{}}
}

func (m *DefaultUserManager) Init(ctx context.Context, location string, encryptionKeyB64 string) error {
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
	m.storePath = filepath.Join(location, usersStoreFile)

	key, encrypted, err := parseKey(encryptionKeyB64)
	if err != nil {
		return err
	}
	m.key = key
	m.encrypted = encrypted
	if !encrypted {
		log.Printf("[knotdb/usermgmt] warning: no encryption key provided; user store is plaintext")
	}

	if _, err := os.Stat(m.storePath); err != nil {
		if os.IsNotExist(err) {
			m.users = []storedUser{}
			m.rebuildIndex()
			return m.persist()
		}
		return err
	}

	raw, err := os.ReadFile(m.storePath)
	if err != nil {
		return err
	}
	users, err := m.decodeStore(raw)
	if err != nil {
		return err
	}
	m.users = users
	m.rebuildIndex()
	return nil
}

func (m *DefaultUserManager) ExistsByRef(ctx context.Context, ref identity.UserRef) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if strings.TrimSpace(string(ref)) == "" {
		return false, fmt.Errorf("%w: user_ref is required", ErrInvalidInput)
	}
	_, ok := m.indexByRefLC[normalizeRef(ref)]
	return ok, nil
}

func (m *DefaultUserManager) GetByRef(ctx context.Context, ref identity.UserRef) (identity.User, error) {
	if err := ctx.Err(); err != nil {
		return identity.User{}, err
	}
	idx, ok := m.indexByRefLC[normalizeRef(ref)]
	if !ok {
		return identity.User{}, ErrUserNotFound
	}
	return m.users[idx].User, nil
}

func (m *DefaultUserManager) Create(ctx context.Context, in CreateUserInput) (identity.User, error) {
	if err := ctx.Err(); err != nil {
		return identity.User{}, err
	}
	if strings.TrimSpace(string(in.User.Ref)) == "" {
		return identity.User{}, fmt.Errorf("%w: user_ref is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.Password) == "" {
		return identity.User{}, fmt.Errorf("%w: password is required", ErrInvalidInput)
	}
	refLC := normalizeRef(in.User.Ref)
	if _, exists := m.indexByRefLC[refLC]; exists {
		return identity.User{}, ErrDuplicateUserRef
	}

	id := uuid.New()
	if in.User.ID != nil {
		id = *in.User.ID
	}
	status := in.User.Status
	if status == "" {
		status = identity.UserStatusPending
	}

	u := identity.User{
		ID:       id,
		Ref:      in.User.Ref,
		Email:    in.User.Email,
		Username: in.User.Username,
		Status:   status,
	}
	m.users = append(m.users, storedUser{User: u, Password: in.Password})
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		return identity.User{}, err
	}
	return u, nil
}

// Authenticate checks credentials and returns a user record.
func (m *DefaultUserManager) Authenticate(ctx context.Context, ref identity.UserRef, password string) (identity.User, error) {
	if err := ctx.Err(); err != nil {
		return identity.User{}, err
	}
	idx, ok := m.indexByRefLC[normalizeRef(ref)]
	if !ok {
		return identity.User{}, ErrUserNotFound
	}
	rec := m.users[idx]
	if rec.Password != password {
		return identity.User{}, ErrInvalidInput
	}
	return rec.User, nil
}

func (m *DefaultUserManager) rebuildIndex() {
	m.indexByRefLC = map[string]int{}
	for i, u := range m.users {
		m.indexByRefLC[normalizeRef(u.User.Ref)] = i
	}
}

func (m *DefaultUserManager) persist() error {
	raw, err := m.encodeStore(m.users)
	if err != nil {
		return err
	}
	return os.WriteFile(m.storePath, raw, 0o600)
}

func (m *DefaultUserManager) encodeStore(users []storedUser) ([]byte, error) {
	plain, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return nil, err
	}
	if !m.encrypted {
		return append(plain, '\n'), nil
	}

	block, err := aes.NewCipher(m.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plain, nil)
	enc := encryptedStore{
		Format:    "knotdb-usermgmt-v1",
		NonceB64:  base64.StdEncoding.EncodeToString(nonce),
		CipherB64: base64.StdEncoding.EncodeToString(ciphertext),
	}
	out, err := json.MarshalIndent(enc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

func (m *DefaultUserManager) decodeStore(raw []byte) ([]storedUser, error) {
	if m.encrypted {
		var enc encryptedStore
		if err := json.Unmarshal(raw, &enc); err != nil {
			return nil, fmt.Errorf("%w: invalid encrypted store format", ErrDecryptFailed)
		}
		if enc.Format != "knotdb-usermgmt-v1" {
			return nil, fmt.Errorf("%w: unsupported encrypted store format", ErrDecryptFailed)
		}
		nonce, err := base64.StdEncoding.DecodeString(enc.NonceB64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid nonce", ErrDecryptFailed)
		}
		ciphertext, err := base64.StdEncoding.DecodeString(enc.CipherB64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid ciphertext", ErrDecryptFailed)
		}
		block, err := aes.NewCipher(m.key)
		if err != nil {
			return nil, err
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		plain, err := gcm.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			return nil, ErrDecryptFailed
		}
		var users []storedUser
		if err := json.Unmarshal(plain, &users); err != nil {
			return nil, err
		}
		return users, nil
	}

	// plaintext mode: tolerate existing encrypted store but return explicit decrypt error
	var maybeEnc encryptedStore
	if err := json.Unmarshal(raw, &maybeEnc); err == nil && maybeEnc.Format == "knotdb-usermgmt-v1" {
		return nil, ErrDecryptFailed
	}
	var users []storedUser
	if err := json.Unmarshal(raw, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func parseKey(keyB64 string) ([]byte, bool, error) {
	k := strings.TrimSpace(keyB64)
	if k == "" {
		return nil, false, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(k)
	if err != nil {
		return nil, false, fmt.Errorf("%w: key must be valid base64", ErrInvalidKey)
	}
	if len(decoded) != 32 {
		return nil, false, fmt.Errorf("%w: decoded key must be 32 bytes", ErrInvalidKey)
	}
	return decoded, true, nil
}

func normalizeRef(ref identity.UserRef) string {
	return strings.ToLower(strings.TrimSpace(string(ref)))
}
