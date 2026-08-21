package disrupttest

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Harness struct {
	Config       Config
	Provisioner  ClusterProvisioner
	Driver       ClusterDriver
	ArtifactRoot string
}

type Summary struct {
	ClusterName string           `json:"clusterName"`
	Namespace   string           `json:"namespace"`
	Profile     string           `json:"profile"`
	SetupOnly   bool             `json:"setupOnly"`
	DryRun      bool             `json:"dryRun"`
	ArtifactDir string           `json:"artifactDir,omitempty"`
	KubeContext string           `json:"kubeContext,omitempty"`
	Scenario    *ScenarioSummary `json:"scenario,omitempty"`
}

func (h *Harness) progressf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[raft-disrupt] "+format+"\n", args...)
}

func NewHarness(cfg Config, runner CommandRunner) (*Harness, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if runner == nil {
		runner = ExecRunner{}
	}
	provisioner := NewK3DProvisioner(cfg.ClusterName, runner)
	return &Harness{Config: cfg, Provisioner: provisioner}, nil
}

func (h *Harness) Run(ctx context.Context) (summary Summary, err error) {
	cfg := h.Config
	if h.ArtifactRoot == "" {
		h.ArtifactRoot = filepath.Join(cfg.ArtifactsDir, time.Now().UTC().Format("20060102-150405")+"-"+cfg.ClusterName)
	}
	summary = Summary{ClusterName: cfg.ClusterName, Namespace: cfg.Namespace, Profile: cfg.Profile, SetupOnly: cfg.SetupOnly, DryRun: cfg.DryRun, ArtifactDir: h.ArtifactRoot}
	if cfg.DryRun {
		h.progressf("running preflight checks")
		return summary, h.Provisioner.Preflight(ctx)
	}
	h.progressf("running preflight checks")
	if err := h.Provisioner.Preflight(ctx); err != nil {
		return summary, err
	}
	clusterCfg, err := h.clusterConfig()
	if err != nil {
		return summary, err
	}
	h.progressf("creating disposable %s cluster %s", h.Provisioner.Name(), clusterCfg.Name)
	kubeCtx, err := h.Provisioner.Create(ctx, clusterCfg)
	if err != nil {
		return summary, err
	}
	summary.KubeContext = kubeCtx.Name
	cleanup := true
	defer func() {
		if cleanup {
			if cleanupErr := h.Provisioner.Delete(context.Background()); cleanupErr != nil {
				if err != nil {
					err = fmt.Errorf("%w; cleanup failed: %v", err, cleanupErr)
				} else {
					err = fmt.Errorf("cleanup failed: %w", cleanupErr)
				}
			}
		}
	}()
	if cfg.PreflightCreateDelete {
		return summary, h.writeConfigArtifact(summary)
	}
	driver := NewK3SDriver(kubeCtx.Name, cfg.Namespace, cfg.Selector, cfg.Service, cfg.StatefulSet, nil)
	h.Driver = driver
	if strings.TrimSpace(cfg.Image) != "" {
		h.progressf("loading image into cluster image=%s", cfg.Image)
		if err := h.Provisioner.LoadImage(ctx, cfg.Image); err != nil {
			return summary, err
		}
	}
	h.progressf("waiting for k3s system readiness")
	if err := driver.WaitSystemReady(ctx); err != nil {
		_ = driver.CollectArtifacts(ctx, h.artifactDir("failure"))
		if cfg.KeepClusterOnFailure {
			cleanup = false
		}
		return summary, err
	}
	manifestCfg := ManifestConfigFromCluster(clusterCfg, cfg.StatefulSet, cfg.Service, cfg.Selector)
	manifests, err := RenderManifests(manifestCfg)
	if err != nil {
		return summary, err
	}
	h.progressf("applying mycel manifests and waiting for pods")
	if err := driver.ApplyManifests(ctx, manifests); err != nil {
		_ = driver.CollectArtifacts(ctx, h.artifactDir("failure"))
		if cfg.KeepClusterOnFailure {
			cleanup = false
		}
		return summary, err
	}
	if err := h.writeConfigArtifact(summary); err != nil {
		return summary, err
	}
	if err := driver.CollectArtifacts(ctx, h.artifactDir("setup")); err != nil {
		return summary, err
	}
	if cfg.SetupOnly {
		return summary, nil
	}
	profile, err := ResolveProfile(cfg.Profile)
	if err != nil {
		return summary, err
	}
	scenario, err := RunScenario(ctx, cfg, profile, driver, clusterCfg.AdminPassword, h.artifactDir("scenario"))
	if scenario.RunID != "" {
		summary.Scenario = &scenario
	}
	if err != nil {
		_ = driver.CollectArtifacts(ctx, h.artifactDir("failure"))
		if cfg.KeepClusterOnFailure {
			cleanup = false
		}
		return summary, err
	}
	return summary, nil
}

func (h *Harness) clusterConfig() (ClusterConfig, error) {
	adminPassword, err := randomB64(24)
	if err != nil {
		return ClusterConfig{}, err
	}
	backendToken, err := randomB64(32)
	if err != nil {
		return ClusterConfig{}, err
	}
	encryptionKey, err := randomB64(32)
	if err != nil {
		return ClusterConfig{}, err
	}
	return ClusterConfig{Name: h.Config.ClusterName, Namespace: h.Config.Namespace, Image: h.Config.Image, AdminUsername: h.Config.AdminUsername, AdminPassword: adminPassword, BackendToken: backendToken, EncryptionKey: encryptionKey, NodeCount: h.Config.NodeCount, PartitionCount: h.Config.PartitionCount}, nil
}

func (h *Harness) artifactDir(kind string) string {
	root := h.ArtifactRoot
	if root == "" {
		root = filepath.Join(h.Config.ArtifactsDir, time.Now().UTC().Format("20060102-150405")+"-"+h.Config.ClusterName)
	}
	return filepath.Join(root, kind)
}

func (h *Harness) writeConfigArtifact(summary Summary) error {
	dir := h.artifactDir("setup")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "summary.json"), data, 0o644)
}

func randomB64(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}
