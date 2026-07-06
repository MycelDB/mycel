package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/identity"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	"github.com/myceldb/mycel/internal/filestore"
)

const storeFile = "embeddings.json"

type storedKey struct {
	ID               domainembedding.ProviderKeyID `json:"id"`
	OwnerID          identity.UserID               `json:"owner_id"`
	ProviderID       string                        `json:"provider_id"`
	Name             string                        `json:"name"`
	IsDefault        bool                          `json:"is_default"`
	Disabled         bool                          `json:"disabled"`
	APIKeyCiphertext string                        `json:"api_key_ciphertext,omitempty"`
	CreatedAt        time.Time                     `json:"created_at"`
	UpdatedAt        time.Time                     `json:"updated_at"`
}

type storedData struct {
	Keys     []storedKey               `json:"keys"`
	Profiles []domainembedding.Profile `json:"profiles"`
}

type encryptedSecret struct {
	NonceB64  string `json:"nonce_b64"`
	CipherB64 string `json:"cipher_b64"`
}

type defaultManager struct {
	mu        sync.RWMutex
	storePath string
	key       []byte
	data      storedData
}

func (m *defaultManager) Init(ctx context.Context, location string, encryptionKeyB64 string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := os.MkdirAll(location, 0o755); err != nil {
		return err
	}
	m.storePath = filepath.Join(location, storeFile)
	key, err := parseOrDeriveKey(encryptionKeyB64, location)
	if err != nil {
		return err
	}
	m.key = key
	if _, err := os.Stat(m.storePath); err != nil {
		if os.IsNotExist(err) {
			m.data = storedData{Keys: []storedKey{}, Profiles: []domainembedding.Profile{}}
			return m.persistLocked()
		}
		return err
	}
	raw, err := os.ReadFile(m.storePath)
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		m.data = storedData{Keys: []storedKey{}, Profiles: []domainembedding.Profile{}}
		return nil
	}
	if err := json.Unmarshal(raw, &m.data); err != nil {
		return err
	}
	return nil
}

func (m *defaultManager) ListKeys(ctx context.Context, ownerID identity.UserID) ([]domainembedding.ProviderKey, error) {
	if err := requireOwner(ctx, ownerID); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []domainembedding.ProviderKey{}
	for _, k := range m.data.Keys {
		if k.OwnerID == ownerID {
			out = append(out, k.toModel())
		}
	}
	return out, nil
}

func (m *defaultManager) GetKey(ctx context.Context, ownerID identity.UserID, id domainembedding.ProviderKeyID) (domainembedding.ProviderKey, error) {
	if err := requireOwner(ctx, ownerID); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx := m.findKeyLocked(ownerID, id)
	if idx < 0 {
		return domainembedding.ProviderKey{}, ErrKeyNotFound
	}
	return m.data.Keys[idx].toModel(), nil
}

func (m *defaultManager) AddKey(ctx context.Context, in AddKeyInput) (domainembedding.ProviderKey, error) {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	if strings.TrimSpace(in.ProviderID) == "" {
		return domainembedding.ProviderKey{}, fmt.Errorf("%w: provider_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.Name) == "" {
		return domainembedding.ProviderKey{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ciphertext := ""
	if strings.TrimSpace(in.APIKey) != "" {
		var err error
		ciphertext, err = m.encryptLocked(in.APIKey)
		if err != nil {
			return domainembedding.ProviderKey{}, err
		}
	}
	now := time.Now().UTC()
	id, err := uuid.NewV7()
	if err != nil {
		return domainembedding.ProviderKey{}, err
	}
	rec := storedKey{ID: id, OwnerID: in.OwnerID, ProviderID: strings.TrimSpace(in.ProviderID), Name: strings.TrimSpace(in.Name), IsDefault: in.IsDefault, Disabled: in.Disabled, APIKeyCiphertext: ciphertext, CreatedAt: now, UpdatedAt: now}
	if rec.IsDefault {
		m.clearDefaultKeysLocked(rec.OwnerID, rec.ProviderID, rec.ID)
	}
	m.data.Keys = append(m.data.Keys, rec)
	if err := m.persistLocked(); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	return rec.toModel(), nil
}

func (m *defaultManager) UpdateKey(ctx context.Context, in UpdateKeyInput) (domainembedding.ProviderKey, error) {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.findKeyLocked(in.OwnerID, in.ID)
	if idx < 0 {
		return domainembedding.ProviderKey{}, ErrKeyNotFound
	}
	rec := m.data.Keys[idx]
	if in.Name != nil {
		if strings.TrimSpace(*in.Name) == "" {
			return domainembedding.ProviderKey{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
		}
		rec.Name = strings.TrimSpace(*in.Name)
	}
	if in.APIKey != nil {
		if strings.TrimSpace(*in.APIKey) == "" {
			rec.APIKeyCiphertext = ""
		} else {
			ciphertext, err := m.encryptLocked(*in.APIKey)
			if err != nil {
				return domainembedding.ProviderKey{}, err
			}
			rec.APIKeyCiphertext = ciphertext
		}
	}
	if in.IsDefault != nil {
		rec.IsDefault = *in.IsDefault
	}
	if in.Disabled != nil {
		rec.Disabled = *in.Disabled
	}
	rec.UpdatedAt = time.Now().UTC()
	if rec.IsDefault {
		m.clearDefaultKeysLocked(rec.OwnerID, rec.ProviderID, rec.ID)
	}
	m.data.Keys[idx] = rec
	if err := m.persistLocked(); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	return rec.toModel(), nil
}

func (m *defaultManager) DeleteKey(ctx context.Context, in DeleteKeyInput) error {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.findKeyLocked(in.OwnerID, in.ID)
	if idx < 0 {
		return ErrKeyNotFound
	}
	m.data.Keys = append(m.data.Keys[:idx], m.data.Keys[idx+1:]...)
	return m.persistLocked()
}

func (m *defaultManager) ResolveAPIKey(ctx context.Context, ownerID identity.UserID, providerID string, keyID domainembedding.ProviderKeyID) (domainembedding.ProviderKey, string, error) {
	if err := requireOwner(ctx, ownerID); err != nil {
		return domainembedding.ProviderKey{}, "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx := -1
	if keyID != uuid.Nil {
		idx = m.findKeyLocked(ownerID, keyID)
	} else {
		for i, k := range m.data.Keys {
			if k.OwnerID == ownerID && k.ProviderID == providerID && !k.Disabled && k.IsDefault {
				idx = i
				break
			}
		}
		if idx < 0 {
			for i, k := range m.data.Keys {
				if k.OwnerID == ownerID && k.ProviderID == providerID && !k.Disabled {
					idx = i
					break
				}
			}
		}
	}
	if idx < 0 {
		return domainembedding.ProviderKey{}, "", ErrKeyNotFound
	}
	rec := m.data.Keys[idx]
	if rec.ProviderID != providerID || rec.Disabled {
		return domainembedding.ProviderKey{}, "", ErrKeyNotFound
	}
	secret := ""
	if rec.APIKeyCiphertext != "" {
		var err error
		secret, err = m.decryptLocked(rec.APIKeyCiphertext)
		if err != nil {
			return domainembedding.ProviderKey{}, "", err
		}
	}
	return rec.toModel(), secret, nil
}

func (m *defaultManager) ListProfiles(ctx context.Context, ownerID identity.UserID) ([]domainembedding.Profile, error) {
	if err := requireOwner(ctx, ownerID); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []domainembedding.Profile{}
	for _, p := range m.data.Profiles {
		if p.OwnerID == ownerID {
			out = append(out, cloneProfile(p))
		}
	}
	return out, nil
}

func (m *defaultManager) GetProfile(ctx context.Context, ownerID identity.UserID, id domainembedding.ProfileID) (domainembedding.Profile, error) {
	if err := requireOwner(ctx, ownerID); err != nil {
		return domainembedding.Profile{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx := m.findProfileLocked(ownerID, id)
	if idx < 0 {
		return domainembedding.Profile{}, ErrProfileNotFound
	}
	return cloneProfile(m.data.Profiles[idx]), nil
}

func (m *defaultManager) AddProfile(ctx context.Context, in AddProfileInput) (domainembedding.Profile, error) {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return domainembedding.Profile{}, err
	}
	if strings.TrimSpace(in.Name) == "" {
		return domainembedding.Profile{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.ProviderID) == "" {
		return domainembedding.Profile{}, fmt.Errorf("%w: provider_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.ModelID) == "" {
		return domainembedding.Profile{}, fmt.Errorf("%w: model_id is required", ErrInvalidInput)
	}
	mode := in.SourceMode
	if mode == "" {
		mode = domainembedding.SourceModeSubtree
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	id, err := uuid.NewV7()
	if err != nil {
		return domainembedding.Profile{}, err
	}
	p := domainembedding.Profile{ID: id, OwnerID: in.OwnerID, Name: strings.TrimSpace(in.Name), ProviderID: strings.TrimSpace(in.ProviderID), ModelID: strings.TrimSpace(in.ModelID), SourceMode: mode, IncludeProps: cleanStrings(in.IncludeProps), MaxDepth: cloneIntPtr(in.MaxDepth), MinimumTextLength: in.MinimumTextLength, TargetTemplateKeys: cleanStrings(in.TargetTemplateKeys), CreatedAt: now, UpdatedAt: now}
	m.data.Profiles = append(m.data.Profiles, p)
	if err := m.persistLocked(); err != nil {
		return domainembedding.Profile{}, err
	}
	return cloneProfile(p), nil
}

func (m *defaultManager) UpdateProfile(ctx context.Context, in UpdateProfileInput) (domainembedding.Profile, error) {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return domainembedding.Profile{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.findProfileLocked(in.OwnerID, in.ID)
	if idx < 0 {
		return domainembedding.Profile{}, ErrProfileNotFound
	}
	p := m.data.Profiles[idx]
	if in.Name != nil {
		if strings.TrimSpace(*in.Name) == "" {
			return domainembedding.Profile{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
		}
		p.Name = strings.TrimSpace(*in.Name)
	}
	if in.ProviderID != nil {
		p.ProviderID = strings.TrimSpace(*in.ProviderID)
	}
	if in.ModelID != nil {
		p.ModelID = strings.TrimSpace(*in.ModelID)
	}
	if in.SourceMode != nil {
		p.SourceMode = *in.SourceMode
	}
	if in.IncludeProps != nil {
		p.IncludeProps = cleanStrings(*in.IncludeProps)
	}
	if in.ClearMaxDepth {
		p.MaxDepth = nil
	} else if in.MaxDepth != nil {
		p.MaxDepth = cloneIntPtr(in.MaxDepth)
	}
	if in.MinimumTextLength != nil {
		p.MinimumTextLength = *in.MinimumTextLength
	}
	if in.TargetTemplateKeys != nil {
		p.TargetTemplateKeys = cleanStrings(*in.TargetTemplateKeys)
	}
	p.UpdatedAt = time.Now().UTC()
	m.data.Profiles[idx] = p
	if err := m.persistLocked(); err != nil {
		return domainembedding.Profile{}, err
	}
	return cloneProfile(p), nil
}

func (m *defaultManager) DeleteProfile(ctx context.Context, in DeleteProfileInput) error {
	if err := requireOwner(ctx, in.OwnerID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.findProfileLocked(in.OwnerID, in.ID)
	if idx < 0 {
		return ErrProfileNotFound
	}
	m.data.Profiles = append(m.data.Profiles[:idx], m.data.Profiles[idx+1:]...)
	return m.persistLocked()
}

func (m *defaultManager) findKeyLocked(ownerID identity.UserID, id domainembedding.ProviderKeyID) int {
	if id == uuid.Nil {
		return -1
	}
	for i, k := range m.data.Keys {
		if k.OwnerID == ownerID && k.ID == id {
			return i
		}
	}
	return -1
}
func (m *defaultManager) findProfileLocked(ownerID identity.UserID, id domainembedding.ProfileID) int {
	if id == uuid.Nil {
		return -1
	}
	for i, p := range m.data.Profiles {
		if p.OwnerID == ownerID && p.ID == id {
			return i
		}
	}
	return -1
}
func (m *defaultManager) clearDefaultKeysLocked(ownerID identity.UserID, providerID string, except domainembedding.ProviderKeyID) {
	for i := range m.data.Keys {
		if m.data.Keys[i].OwnerID == ownerID && m.data.Keys[i].ProviderID == providerID && m.data.Keys[i].ID != except {
			m.data.Keys[i].IsDefault = false
		}
	}
}
func (m *defaultManager) persistLocked() error {
	raw, err := json.MarshalIndent(m.data, "", "  ")
	if err != nil {
		return err
	}
	return filestore.WriteFileAtomic(m.storePath, append(raw, '\n'), 0o600)
}
func (k storedKey) toModel() domainembedding.ProviderKey {
	return domainembedding.ProviderKey{ID: k.ID, OwnerID: k.OwnerID, ProviderID: k.ProviderID, Name: k.Name, IsDefault: k.IsDefault, Disabled: k.Disabled, HasAPIKey: k.APIKeyCiphertext != "", CreatedAt: k.CreatedAt, UpdatedAt: k.UpdatedAt}
}
func (m *defaultManager) encryptLocked(secret string) (string, error) {
	block, err := aes.NewCipher(m.key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	enc := encryptedSecret{NonceB64: base64.StdEncoding.EncodeToString(nonce), CipherB64: base64.StdEncoding.EncodeToString(gcm.Seal(nil, nonce, []byte(secret), nil))}
	raw, err := json.Marshal(enc)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
func (m *defaultManager) decryptLocked(ciphertext string) (string, error) {
	var enc encryptedSecret
	if err := json.Unmarshal([]byte(ciphertext), &enc); err != nil {
		return "", ErrDecryptFailed
	}
	nonce, err := base64.StdEncoding.DecodeString(enc.NonceB64)
	if err != nil {
		return "", ErrDecryptFailed
	}
	data, err := base64.StdEncoding.DecodeString(enc.CipherB64)
	if err != nil {
		return "", ErrDecryptFailed
	}
	block, err := aes.NewCipher(m.key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, nonce, data, nil)
	if err != nil {
		return "", ErrDecryptFailed
	}
	return string(plain), nil
}
func parseOrDeriveKey(keyB64, location string) ([]byte, error) {
	if strings.TrimSpace(keyB64) != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(keyB64))
		if err != nil {
			return nil, fmt.Errorf("%w: key must be base64", ErrInvalidKey)
		}
		if len(decoded) != 32 {
			return nil, fmt.Errorf("%w: decoded key must be 32 bytes", ErrInvalidKey)
		}
		return decoded, nil
	}
	sum := sha256.Sum256([]byte("mycel-embedding-secret-v1:" + filepath.Clean(location)))
	return sum[:], nil
}
func requireOwner(ctx context.Context, ownerID identity.UserID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ownerID == uuid.Nil {
		return fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	return nil
}
func cleanStrings(in []string) []string {
	out := []string{}
	for _, v := range in {
		if s := strings.TrimSpace(v); s != "" {
			out = append(out, s)
		}
	}
	return out
}
func cloneIntPtr(v *int) *int {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}
func cloneProfile(p domainembedding.Profile) domainembedding.Profile {
	p.IncludeProps = append([]string(nil), p.IncludeProps...)
	p.TargetTemplateKeys = append([]string(nil), p.TargetTemplateKeys...)
	p.MaxDepth = cloneIntPtr(p.MaxDepth)
	return p
}
