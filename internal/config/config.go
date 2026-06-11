package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/spf13/pflag"
	knotengine "martinbeauvais.com/mbgit/knotbase/knotdb/engine"
)

const EnvConfig = "KNOTDB_CONFIG"

type Options struct {
	ConfigFile string
	Flags      *pflag.FlagSet
}

type Config struct {
	ConfigFile                string
	DataDir                   string
	Output                    string
	CreateIfMissing           bool
	AdminUsername             string
	AdminPassword             string
	UserStoreEncryptionKeyB64 string
	AccessTokenTTL            time.Duration
	BlobStaleTmpAge           time.Duration
	BlobLimits                knotengine.BlobLimits
}

func Load(opts Options) (Config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults(), "."), nil); err != nil {
		return Config{}, err
	}
	configFile := firstNonEmpty(opts.ConfigFile, os.Getenv(EnvConfig))
	if opts.Flags != nil {
		if f := opts.Flags.Lookup("config"); f != nil && f.Changed {
			configFile = f.Value.String()
		}
	}
	if strings.TrimSpace(configFile) != "" {
		if err := k.Load(file.Provider(configFile), yaml.Parser()); err != nil {
			return Config{}, err
		}
	}
	if err := k.Load(env.Provider("KNOTDB_", ".", envKey), nil); err != nil {
		return Config{}, err
	}
	applyEnvAliases(k)
	applyFlagOverrides(k, opts.Flags)

	cfg := Config{
		ConfigFile:                configFile,
		DataDir:                   strings.TrimSpace(k.String("data_dir")),
		Output:                    strings.TrimSpace(k.String("output")),
		CreateIfMissing:           k.Bool("create_if_missing"),
		AdminUsername:             strings.TrimSpace(k.String("admin_username")),
		AdminPassword:             k.String("admin_password"),
		UserStoreEncryptionKeyB64: strings.TrimSpace(k.String("security.user_store_encryption_key_b64")),
		AccessTokenTTL:            k.Duration("auth.access_token_ttl"),
		BlobStaleTmpAge:           k.Duration("storage.blobs.stale_tmp_age"),
		BlobLimits: knotengine.BlobLimits{
			MaxSizeBytes:   k.Int64("storage.blobs.max_size_bytes"),
			MaxImageBytes:  k.Int64("storage.blobs.max_image_bytes"),
			MaxPDFBytes:    k.Int64("storage.blobs.max_pdf_bytes"),
			MaxAudioBytes:  k.Int64("storage.blobs.max_audio_bytes"),
			MaxVideoBytes:  k.Int64("storage.blobs.max_video_bytes"),
			MaxOtherBytes:  k.Int64("storage.blobs.max_other_bytes"),
			MimeTypeLimits: normalizeMimeLimits(int64Map(k.Get("storage.blobs.mime_type_limits"))),
		},
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) EngineConfig() knotengine.EngineConfig {
	return knotengine.EngineConfig{
		DataDir:                   c.DataDir,
		Mode:                      knotengine.EngineModeStandalone,
		CreateIfMissing:           c.CreateIfMissing,
		AdminUsername:             c.AdminUsername,
		AdminPassword:             c.AdminPassword,
		UserStoreEncryptionKeyB64: c.UserStoreEncryptionKeyB64,
		AccessTokenTTL:            c.AccessTokenTTL,
		BlobLimits:                c.BlobLimits,
		BlobStaleTmpAge:           c.BlobStaleTmpAge,
	}
}

func (c Config) Validate() error {
	if c.Output != "" && c.Output != "text" && c.Output != "json" {
		return fmt.Errorf("invalid output %q: expected text or json", c.Output)
	}
	if c.AccessTokenTTL <= 0 {
		return fmt.Errorf("auth.access_token_ttl must be positive")
	}
	if c.BlobStaleTmpAge <= 0 {
		return fmt.Errorf("storage.blobs.stale_tmp_age must be positive")
	}
	return validateBlobLimits(c.BlobLimits)
}

func defaults() map[string]any {
	return map[string]any{
		"data_dir":                               "",
		"output":                                 "text",
		"create_if_missing":                      false,
		"admin_username":                         "",
		"admin_password":                         "",
		"security.user_store_encryption_key_b64": "",
		"auth.access_token_ttl":                  "1h",
		"storage.blobs.stale_tmp_age":            "1h",
		"storage.blobs.max_size_bytes":           int64(-1),
		"storage.blobs.max_image_bytes":          int64(-1),
		"storage.blobs.max_pdf_bytes":            int64(-1),
		"storage.blobs.max_audio_bytes":          int64(-1),
		"storage.blobs.max_video_bytes":          int64(-1),
		"storage.blobs.max_other_bytes":          int64(-1),
		"storage.blobs.mime_type_limits":         map[string]int64{},
	}
}

func envKey(key string) string {
	key = strings.TrimPrefix(key, "KNOTDB_")
	key = strings.ToLower(key)
	key = strings.ReplaceAll(key, "__", ".")
	return key
}

func applyEnvAliases(k *koanf.Koanf) {
	aliases := map[string]string{
		"KNOTDB_DATA_DIR":                      "data_dir",
		"KNOTDB_USER_STORE_ENCRYPTION_KEY_B64": "security.user_store_encryption_key_b64",
		"KNOTDB_AUTH_ACCESS_TOKEN_TTL":         "auth.access_token_ttl",
		"KNOTDB_STORAGE_BLOBS_STALE_TMP_AGE":   "storage.blobs.stale_tmp_age",
		"KNOTDB_STORAGE_BLOBS_MAX_SIZE_BYTES":  "storage.blobs.max_size_bytes",
		"KNOTDB_STORAGE_BLOBS_MAX_IMAGE_BYTES": "storage.blobs.max_image_bytes",
		"KNOTDB_STORAGE_BLOBS_MAX_PDF_BYTES":   "storage.blobs.max_pdf_bytes",
		"KNOTDB_STORAGE_BLOBS_MAX_AUDIO_BYTES": "storage.blobs.max_audio_bytes",
		"KNOTDB_STORAGE_BLOBS_MAX_VIDEO_BYTES": "storage.blobs.max_video_bytes",
		"KNOTDB_STORAGE_BLOBS_MAX_OTHER_BYTES": "storage.blobs.max_other_bytes",
	}
	for envName, key := range aliases {
		if value := strings.TrimSpace(os.Getenv(envName)); value != "" {
			_ = k.Set(key, value)
		}
	}
}

func applyFlagOverrides(k *koanf.Koanf, flags *pflag.FlagSet) {
	if flags == nil {
		return
	}
	flagMap := map[string]string{
		"data-dir":                      "data_dir",
		"output":                        "output",
		"username":                      "admin_username",
		"password":                      "admin_password",
		"user-store-encryption-key-b64": "security.user_store_encryption_key_b64",
		"auth-token-ttl":                "auth.access_token_ttl",
		"blob-stale-tmp-age":            "storage.blobs.stale_tmp_age",
		"blob-max-size-bytes":           "storage.blobs.max_size_bytes",
		"blob-max-image-bytes":          "storage.blobs.max_image_bytes",
		"blob-max-pdf-bytes":            "storage.blobs.max_pdf_bytes",
		"blob-max-audio-bytes":          "storage.blobs.max_audio_bytes",
		"blob-max-video-bytes":          "storage.blobs.max_video_bytes",
		"blob-max-other-bytes":          "storage.blobs.max_other_bytes",
	}
	for flagName, key := range flagMap {
		if f := flags.Lookup(flagName); f != nil && f.Changed {
			_ = k.Set(key, f.Value.String())
		}
	}
}

func validateBlobLimits(limits knotengine.BlobLimits) error {
	values := map[string]int64{
		"storage.blobs.max_size_bytes":  limits.MaxSizeBytes,
		"storage.blobs.max_image_bytes": limits.MaxImageBytes,
		"storage.blobs.max_pdf_bytes":   limits.MaxPDFBytes,
		"storage.blobs.max_audio_bytes": limits.MaxAudioBytes,
		"storage.blobs.max_video_bytes": limits.MaxVideoBytes,
		"storage.blobs.max_other_bytes": limits.MaxOtherBytes,
	}
	for key, value := range values {
		if value < -1 {
			return fmt.Errorf("%s must be -1, 0, or a positive byte count", key)
		}
		if limits.MaxSizeBytes > 0 && value > limits.MaxSizeBytes {
			return fmt.Errorf("%s must not exceed storage.blobs.max_size_bytes", key)
		}
	}
	for mimeType, value := range limits.MimeTypeLimits {
		if strings.TrimSpace(mimeType) == "" {
			return fmt.Errorf("storage.blobs.mime_type_limits contains an empty MIME type")
		}
		if value < -1 {
			return fmt.Errorf("storage.blobs.mime_type_limits[%s] must be -1, 0, or a positive byte count", mimeType)
		}
		if limits.MaxSizeBytes > 0 && value > limits.MaxSizeBytes {
			return fmt.Errorf("storage.blobs.mime_type_limits[%s] must not exceed storage.blobs.max_size_bytes", mimeType)
		}
	}
	return nil
}

func normalizeMimeLimits(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[strings.ToLower(strings.TrimSpace(key))] = value
	}
	return out
}

func int64Map(raw any) map[string]int64 {
	out := map[string]int64{}
	switch values := raw.(type) {
	case map[string]int64:
		for key, value := range values {
			out[key] = value
		}
	case map[string]any:
		for key, value := range values {
			switch typed := value.(type) {
			case int:
				out[key] = int64(typed)
			case int64:
				out[key] = typed
			case float64:
				out[key] = int64(typed)
			case string:
				var parsed int64
				if _, err := fmt.Sscan(strings.TrimSpace(typed), &parsed); err == nil {
					out[key] = parsed
				}
			}
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
