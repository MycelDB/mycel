package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/spf13/pflag"
)

const EnvConfig = "MYCEL_CONFIG"

type Options struct {
	ConfigFile string
	Flags      *pflag.FlagSet
}

type Config struct {
	ConfigFile                  string
	Output                      string
	Username                    string
	Password                    string
	DaemonAddr                  string
	DaemonTLS                   bool
	DaemonTLSCAFile             string
	DaemonTLSServerName         string
	DaemonTLSInsecureSkipVerify bool
	DaemonTLSClientCertFile     string
	DaemonTLSClientKeyFile      string
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
	applyEnvOverrides(k)
	applyFlagOverrides(k, opts.Flags)

	cfg := Config{
		ConfigFile:                  configFile,
		Output:                      strings.TrimSpace(k.String("output")),
		Username:                    strings.TrimSpace(k.String("username")),
		Password:                    k.String("password"),
		DaemonAddr:                  strings.TrimSpace(k.String("daemon.addr")),
		DaemonTLS:                   k.Bool("daemon.tls"),
		DaemonTLSCAFile:             strings.TrimSpace(k.String("daemon.tls_ca")),
		DaemonTLSServerName:         strings.TrimSpace(k.String("daemon.tls_server_name")),
		DaemonTLSInsecureSkipVerify: k.Bool("daemon.tls_insecure_skip_verify"),
		DaemonTLSClientCertFile:     strings.TrimSpace(k.String("daemon.tls_client_cert")),
		DaemonTLSClientKeyFile:      strings.TrimSpace(k.String("daemon.tls_client_key")),
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Output != "" && c.Output != "text" && c.Output != "json" {
		return fmt.Errorf("invalid output %q: expected text or json", c.Output)
	}
	if (c.DaemonTLSClientCertFile == "") != (c.DaemonTLSClientKeyFile == "") {
		return fmt.Errorf("daemon TLS client cert and key must be set together")
	}
	return nil
}

func defaults() map[string]any {
	return map[string]any{
		"output":                          "text",
		"username":                        "",
		"password":                        "",
		"daemon.addr":                     "",
		"daemon.tls":                      false,
		"daemon.tls_ca":                   "",
		"daemon.tls_server_name":          "",
		"daemon.tls_insecure_skip_verify": false,
		"daemon.tls_client_cert":          "",
		"daemon.tls_client_key":           "",
	}
}

func applyEnvOverrides(k *koanf.Koanf) {
	aliases := map[string]string{
		"MYCEL_OUTPUT":                    "output",
		"MYCEL_USERNAME":                  "username",
		"MYCEL_PASSWORD":                  "password",
		"MYCELD_GRPC_ADDR":                "daemon.addr",
		"MYCELD_TLS":                      "daemon.tls",
		"MYCELD_TLS_CA_FILE":              "daemon.tls_ca",
		"MYCELD_TLS_SERVER_NAME":          "daemon.tls_server_name",
		"MYCELD_TLS_INSECURE_SKIP_VERIFY": "daemon.tls_insecure_skip_verify",
		"MYCELD_TLS_CLIENT_CERT_FILE":     "daemon.tls_client_cert",
		"MYCELD_TLS_CLIENT_KEY_FILE":      "daemon.tls_client_key",
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
		"output":                          "output",
		"username":                        "username",
		"password":                        "password",
		"daemon-addr":                     "daemon.addr",
		"daemon-tls":                      "daemon.tls",
		"daemon-tls-ca":                   "daemon.tls_ca",
		"daemon-tls-server-name":          "daemon.tls_server_name",
		"daemon-tls-insecure-skip-verify": "daemon.tls_insecure_skip_verify",
		"daemon-tls-client-cert":          "daemon.tls_client_cert",
		"daemon-tls-client-key":           "daemon.tls_client_key",
	}
	for flagName, key := range flagMap {
		if f := flags.Lookup(flagName); f != nil && f.Changed {
			_ = k.Set(key, f.Value.String())
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
