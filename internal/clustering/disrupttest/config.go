package disrupttest

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultDriver       = "k3s"
	DefaultProvisioner  = "k3d"
	DefaultProfile      = "smoke"
	DefaultNamespace    = "mycel-raft-disrupt"
	DefaultSelector     = "app=myceld"
	DefaultService      = "myceld"
	DefaultStatefulSet  = "myceld"
	DefaultImage        = "myceldb/mycel:raft-disrupt-local"
	DefaultAdminUser    = "admin"
	DefaultPartitionCnt = 16
	DefaultNodeCount    = 3
)

type Config struct {
	Driver                string
	Provisioner           string
	ClusterName           string
	Namespace             string
	Selector              string
	Service               string
	StatefulSet           string
	Image                 string
	AdminUsername         string
	AdminPasswordFile     string
	Profile               string
	RestartNode           string
	ArtifactsDir          string
	ScenarioFile          string
	Workload              string
	KeepClusterOnFailure  bool
	ConfirmDestructive    bool
	DryRun                bool
	SetupOnly             bool
	NoDisruption          bool
	PreflightCreateDelete bool
	PartitionCount        int
	NodeCount             int
	Now                   time.Time
}

type ClusterConfig struct {
	Name           string
	Namespace      string
	Image          string
	AdminUsername  string
	AdminPassword  string
	BackendToken   string
	EncryptionKey  string
	NodeCount      int
	PartitionCount int
}

type Profile struct {
	Name     string
	Duration time.Duration
	Writers  int
	Rate     int
}

func DefaultConfig(now time.Time) Config {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	suffix := now.Format("20060102-150405")
	return Config{
		Driver:         DefaultDriver,
		Provisioner:    DefaultProvisioner,
		ClusterName:    "mycel-rdt-" + suffix,
		Namespace:      DefaultNamespace,
		Selector:       DefaultSelector,
		Service:        DefaultService,
		StatefulSet:    DefaultStatefulSet,
		Image:          DefaultImage,
		AdminUsername:  DefaultAdminUser,
		Profile:        DefaultProfile,
		RestartNode:    "",
		ArtifactsDir:   "artifacts/raft-disruption",
		Workload:       "nodes",
		PartitionCount: DefaultPartitionCnt,
		NodeCount:      DefaultNodeCount,
		Now:            now,
	}
}

func ConfigFromEnv(now time.Time) Config {
	cfg := DefaultConfig(now)
	setString(&cfg.Driver, "MYCEL_DISRUPT_DRIVER")
	setString(&cfg.Provisioner, "MYCEL_DISRUPT_PROVISIONER")
	setString(&cfg.ClusterName, "MYCEL_DISRUPT_CLUSTER_NAME")
	setString(&cfg.Namespace, "MYCEL_K3S_NAMESPACE")
	setString(&cfg.Selector, "MYCEL_K3S_SELECTOR")
	setString(&cfg.Service, "MYCEL_K3S_SERVICE")
	setString(&cfg.Image, "MYCEL_DISRUPT_IMAGE")
	setString(&cfg.AdminUsername, "MYCEL_ADMIN_USERNAME")
	setString(&cfg.AdminPasswordFile, "MYCEL_ADMIN_PASSWORD_FILE")
	setString(&cfg.Profile, "MYCEL_DISRUPT_PROFILE")
	setString(&cfg.ArtifactsDir, "MYCEL_DISRUPT_ARTIFACTS_DIR")
	setString(&cfg.ScenarioFile, "MYCEL_DISRUPT_SCENARIO_FILE")
	setString(&cfg.Workload, "MYCEL_DISRUPT_WORKLOAD")
	setInt(&cfg.PartitionCount, "MYCEL_DISRUPT_PARTITION_COUNT")
	setInt(&cfg.NodeCount, "MYCEL_DISRUPT_NODE_COUNT")
	cfg.KeepClusterOnFailure = envBool("MYCEL_KEEP_CLUSTER_ON_FAILURE", cfg.KeepClusterOnFailure)
	cfg.ConfirmDestructive = envBool("MYCEL_CONFIRM_DESTRUCTIVE", cfg.ConfirmDestructive)
	cfg.NoDisruption = envBool("MYCEL_DISRUPT_NO_DISRUPTION", cfg.NoDisruption)
	return cfg
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Driver) != "k3s" {
		return fmt.Errorf("unsupported driver %q", c.Driver)
	}
	if strings.TrimSpace(c.Provisioner) != "k3d" {
		return fmt.Errorf("unsupported provisioner %q", c.Provisioner)
	}
	if len(strings.TrimSpace(c.ClusterName)) > 32 {
		return fmt.Errorf("cluster name must be <= 32 characters for k3d")
	}
	for name, value := range map[string]string{"cluster name": c.ClusterName, "namespace": c.Namespace, "selector": c.Selector, "service": c.Service, "statefulset": c.StatefulSet, "image": c.Image, "admin username": c.AdminUsername, "profile": c.Profile, "artifacts dir": c.ArtifactsDir, "workload": c.Workload} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if _, err := appSelectorValue(c.Selector); err != nil {
		return err
	}
	if c.NodeCount != 3 {
		return fmt.Errorf("node count must be 3 for the initial disruption harness")
	}
	if c.PartitionCount <= 0 {
		return fmt.Errorf("partition count must be positive")
	}
	if _, err := ResolveProfile(c.Profile); err != nil {
		return err
	}
	if !IsSupportedWorkload(strings.TrimSpace(c.Workload)) {
		return fmt.Errorf("unsupported workload %q", c.Workload)
	}
	if !c.ConfirmDestructive && !c.DryRun {
		return fmt.Errorf("--confirm-destructive is required for cluster create/delete/restart")
	}
	return nil
}

func ResolveProfile(name string) (Profile, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "smoke":
		return Profile{Name: "smoke", Duration: 30 * time.Second, Writers: 1, Rate: 5}, nil
	case "small":
		return Profile{Name: "small", Duration: 2 * time.Minute, Writers: 2, Rate: 20}, nil
	case "medium":
		return Profile{Name: "medium", Duration: 10 * time.Minute, Writers: 4, Rate: 50}, nil
	case "soak":
		return Profile{Name: "soak", Duration: time.Hour, Writers: 8, Rate: 100}, nil
	default:
		return Profile{}, fmt.Errorf("unsupported pressure profile %q", name)
	}
}

func appSelectorValue(selector string) (string, error) {
	selector = strings.TrimSpace(selector)
	key, value, ok := strings.Cut(selector, "=")
	if !ok || strings.TrimSpace(key) != "app" || strings.TrimSpace(value) == "" || strings.Contains(value, ",") {
		return "", fmt.Errorf("selector must be a single app=<value> selector")
	}
	return strings.TrimSpace(value), nil
}

func setString(target *string, env string) {
	if value := strings.TrimSpace(os.Getenv(env)); value != "" {
		*target = value
	}
}

func setInt(target *int, env string) {
	value := strings.TrimSpace(os.Getenv(env))
	if value == "" {
		return
	}
	parsed, err := strconv.Atoi(value)
	if err == nil {
		*target = parsed
	}
}

func envBool(env string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(env)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
}
