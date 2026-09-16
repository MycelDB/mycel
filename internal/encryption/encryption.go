package encryption

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const (
	ModeDisabled = "disabled"
	ModeEnabled  = "enabled"

	ProviderStaticEnv  = "static-env"
	ProviderStaticFile = "static-file"
	ProviderVault      = "vault-transit"
	ProviderCloudKMS   = "cloud-kms"

	AlgorithmAES256GCM = "AES-256-GCM"

	envelopeMagic   = "MYCELENC"
	envelopeVersion = uint16(1)
	wrapKeyID       = "static-v1"
)

var (
	ErrDisabled            = errors.New("encryption is disabled")
	ErrInvalidConfig       = errors.New("invalid encryption config")
	ErrUnsupportedProvider = errors.New("unsupported encryption key provider")
	ErrInvalidKey          = errors.New("invalid encryption key")
	ErrInvalidEnvelope     = errors.New("invalid encryption envelope")
	ErrAuthentication      = errors.New("encrypted payload authentication failed")
)

type Config struct {
	AtRest        string
	KEKProvider   string
	StaticKeyB64  string
	StaticKeyFile string
	DEKCacheTTL   time.Duration
}

func (c Config) Enabled() bool {
	return strings.EqualFold(strings.TrimSpace(c.AtRest), ModeEnabled)
}

func (c Config) NormalizedMode() string {
	mode := strings.ToLower(strings.TrimSpace(c.AtRest))
	if mode == "" {
		return ModeDisabled
	}
	return mode
}

func (c Config) Validate() error {
	switch c.NormalizedMode() {
	case ModeDisabled:
		return nil
	case ModeEnabled:
	default:
		return fmt.Errorf("%w: MYCELD_ENCRYPTION_AT_REST must be disabled or enabled", ErrInvalidConfig)
	}
	provider := strings.ToLower(strings.TrimSpace(c.KEKProvider))
	switch provider {
	case ProviderStaticEnv:
		if strings.TrimSpace(c.StaticKeyB64) == "" {
			return fmt.Errorf("%w: MYCELD_ENCRYPTION_STATIC_KEY_B64 is required when MYCELD_ENCRYPTION_KEK_PROVIDER=static-env", ErrInvalidConfig)
		}
	case ProviderStaticFile:
		if strings.TrimSpace(c.StaticKeyFile) == "" {
			return fmt.Errorf("%w: MYCELD_ENCRYPTION_STATIC_KEY_FILE is required when MYCELD_ENCRYPTION_KEK_PROVIDER=static-file", ErrInvalidConfig)
		}
	case ProviderVault, ProviderCloudKMS:
		return fmt.Errorf("%w: %s provider is planned but not implemented", ErrUnsupportedProvider, provider)
	case "":
		return fmt.Errorf("%w: MYCELD_ENCRYPTION_KEK_PROVIDER is required when encryption is enabled", ErrInvalidConfig)
	default:
		return fmt.Errorf("%w: MYCELD_ENCRYPTION_KEK_PROVIDER must be static-env, static-file, vault-transit, or cloud-kms", ErrInvalidConfig)
	}
	if c.DEKCacheTTL < 0 {
		return fmt.Errorf("%w: MYCELD_ENCRYPTION_DEK_CACHE_TTL must be positive", ErrInvalidConfig)
	}
	return nil
}

type WrappedKey struct {
	ProviderID string
	KeyID      string
	Nonce      []byte
	Ciphertext []byte
}

type KeyProvider interface {
	ProviderID() string
	ActiveKeyID(ctx context.Context) (string, error)
	WrapKey(ctx context.Context, keyID string, plaintextDEK []byte, aad []byte) (WrappedKey, error)
	UnwrapKey(ctx context.Context, wrapped WrappedKey, aad []byte) ([]byte, error)
}

type Service struct {
	cfg      Config
	provider KeyProvider
}

func NewService(ctx context.Context, cfg Config) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled() {
		return &Service{cfg: cfg}, nil
	}
	provider, err := NewKeyProvider(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if _, err := provider.ActiveKeyID(ctx); err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, provider: provider}, nil
}

func NewKeyProvider(ctx context.Context, cfg Config) (KeyProvider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(cfg.KEKProvider)) {
	case ProviderStaticEnv:
		key, err := decodeKey(cfg.StaticKeyB64)
		if err != nil {
			return nil, err
		}
		return &staticProvider{id: ProviderStaticEnv, key: key}, nil
	case ProviderStaticFile:
		data, err := os.ReadFile(strings.TrimSpace(cfg.StaticKeyFile))
		if err != nil {
			return nil, fmt.Errorf("read static encryption key file: %w", err)
		}
		key, err := decodeKey(string(data))
		if err != nil {
			return nil, err
		}
		return &staticProvider{id: ProviderStaticFile, key: key}, nil
	case ProviderVault, ProviderCloudKMS:
		return nil, fmt.Errorf("%w: %s provider is planned but not implemented", ErrUnsupportedProvider, cfg.KEKProvider)
	default:
		return nil, fmt.Errorf("%w: MYCELD_ENCRYPTION_KEK_PROVIDER is required", ErrInvalidConfig)
	}
}

func (s *Service) Enabled() bool { return s != nil && s.cfg.Enabled() }

func (s *Service) Mode() string {
	if s == nil {
		return ModeDisabled
	}
	return s.cfg.NormalizedMode()
}

func (s *Service) ProviderID() string {
	if s == nil || s.provider == nil {
		return ""
	}
	return s.provider.ProviderID()
}

func (s *Service) ActiveKeyID(ctx context.Context) (string, error) {
	if !s.Enabled() || s.provider == nil {
		return "", ErrDisabled
	}
	return s.provider.ActiveKeyID(ctx)
}

func (s *Service) EncryptRecord(ctx context.Context, plaintext []byte, aad []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.Enabled() || s.provider == nil {
		return append([]byte(nil), plaintext...), nil
	}
	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, fmt.Errorf("generate DEK: %w", err)
	}
	keyID, err := s.provider.ActiveKeyID(ctx)
	if err != nil {
		return nil, err
	}
	wrapped, err := s.provider.WrapKey(ctx, keyID, dek, aad)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, aad)
	return encodeEnvelope(envelope{
		ProviderID: wrapped.ProviderID,
		KeyID:      wrapped.KeyID,
		WrapNonce:  wrapped.Nonce,
		WrappedDEK: wrapped.Ciphertext,
		Nonce:      nonce,
		AAD:        append([]byte(nil), aad...),
		Ciphertext: ciphertext,
	})
}

func (s *Service) DecryptRecord(ctx context.Context, payload []byte, expectedAAD []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !IsEnvelope(payload) {
		if s.Enabled() {
			return nil, fmt.Errorf("%w: plaintext payload in encrypted deployment", ErrInvalidEnvelope)
		}
		return append([]byte(nil), payload...), nil
	}
	if !s.Enabled() || s.provider == nil {
		return nil, fmt.Errorf("%w: encrypted payload requires configured key provider", ErrDisabled)
	}
	env, err := decodeEnvelope(payload)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(env.AAD, expectedAAD) {
		return nil, fmt.Errorf("%w: AAD mismatch", ErrAuthentication)
	}
	wrapped := WrappedKey{ProviderID: env.ProviderID, KeyID: env.KeyID, Nonce: env.WrapNonce, Ciphertext: env.WrappedDEK}
	dek, err := s.provider.UnwrapKey(ctx, wrapped, env.AAD)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, env.Nonce, env.Ciphertext, env.AAD)
	if err != nil {
		return nil, ErrAuthentication
	}
	return plaintext, nil
}

func IsEnvelope(payload []byte) bool {
	return len(payload) >= len(envelopeMagic) && string(payload[:len(envelopeMagic)]) == envelopeMagic
}

type staticProvider struct {
	id  string
	key []byte
}

func (p *staticProvider) ProviderID() string                          { return p.id }
func (p *staticProvider) ActiveKeyID(context.Context) (string, error) { return wrapKeyID, nil }

func (p *staticProvider) WrapKey(ctx context.Context, keyID string, plaintextDEK []byte, aad []byte) (WrappedKey, error) {
	if err := ctx.Err(); err != nil {
		return WrappedKey{}, err
	}
	if keyID == "" {
		keyID = wrapKeyID
	}
	block, err := aes.NewCipher(p.key)
	if err != nil {
		return WrappedKey{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return WrappedKey{}, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return WrappedKey{}, fmt.Errorf("generate wrap nonce: %w", err)
	}
	return WrappedKey{ProviderID: p.id, KeyID: keyID, Nonce: nonce, Ciphertext: gcm.Seal(nil, nonce, plaintextDEK, aad)}, nil
}

func (p *staticProvider) UnwrapKey(ctx context.Context, wrapped WrappedKey, aad []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if wrapped.ProviderID != p.id {
		return nil, fmt.Errorf("%w: provider mismatch", ErrInvalidEnvelope)
	}
	block, err := aes.NewCipher(p.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, wrapped.Nonce, wrapped.Ciphertext, aad)
	if err != nil {
		return nil, ErrAuthentication
	}
	if len(plaintext) != 32 {
		return nil, ErrInvalidKey
	}
	return plaintext, nil
}

func decodeKey(value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("%w: key must be base64", ErrInvalidKey)
	}
	if len(decoded) != 32 {
		return nil, fmt.Errorf("%w: key must decode to 32 bytes", ErrInvalidKey)
	}
	return decoded, nil
}

type envelope struct {
	ProviderID string
	KeyID      string
	WrapNonce  []byte
	WrappedDEK []byte
	Nonce      []byte
	AAD        []byte
	Ciphertext []byte
}

func encodeEnvelope(env envelope) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(envelopeMagic)
	_ = binary.Write(&buf, binary.BigEndian, envelopeVersion)
	fields := [][]byte{[]byte(env.ProviderID), []byte(env.KeyID), env.WrapNonce, env.WrappedDEK, env.Nonce, env.AAD, env.Ciphertext}
	for _, field := range fields {
		if len(field) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: field too large", ErrInvalidEnvelope)
		}
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(field)))
		buf.Write(field)
	}
	return buf.Bytes(), nil
}

func decodeEnvelope(payload []byte) (envelope, error) {
	if !IsEnvelope(payload) {
		return envelope{}, ErrInvalidEnvelope
	}
	r := bytes.NewReader(payload[len(envelopeMagic):])
	var version uint16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return envelope{}, err
	}
	if version != envelopeVersion {
		return envelope{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidEnvelope, version)
	}
	fields := make([][]byte, 7)
	for i := range fields {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return envelope{}, err
		}
		fields[i] = make([]byte, n)
		if _, err := io.ReadFull(r, fields[i]); err != nil {
			return envelope{}, err
		}
	}
	if r.Len() != 0 {
		return envelope{}, fmt.Errorf("%w: trailing bytes", ErrInvalidEnvelope)
	}
	return envelope{ProviderID: string(fields[0]), KeyID: string(fields[1]), WrapNonce: fields[2], WrappedDEK: fields[3], Nonce: fields[4], AAD: fields[5], Ciphertext: fields[6]}, nil
}
