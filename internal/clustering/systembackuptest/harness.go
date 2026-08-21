package systembackuptest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	backupcluster "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/clustering/disrupttest"
)

type Harness struct {
	Config       Config
	Provisioner  disrupttest.ClusterProvisioner
	Driver       *disrupttest.K3SDriver
	Runner       disrupttest.CommandRunner
	ArtifactRoot string
}

type Summary struct {
	Status             string                                `json:"status"`
	Error              string                                `json:"error,omitempty"`
	ClusterName        string                                `json:"clusterName"`
	Namespace          string                                `json:"namespace"`
	Profile            string                                `json:"profile"`
	Workload           string                                `json:"workload"`
	ArtifactDir        string                                `json:"artifactDir"`
	BackupSetID        string                                `json:"backupSetId,omitempty"`
	AttemptedWrites    int64                                 `json:"attemptedWrites,omitempty"`
	SuccessfulWrites   int64                                 `json:"successfulWrites,omitempty"`
	AmbiguousWrites    int64                                 `json:"ambiguousWrites,omitempty"`
	PVCsDeleted        int                                   `json:"pvcsDeleted,omitempty"`
	PVCsRestored       int                                   `json:"pvcsRestored,omitempty"`
	OldPVCUIDs         map[string]string                     `json:"oldPvcUids,omitempty"`
	NewPVCUIDs         map[string]string                     `json:"newPvcUids,omitempty"`
	PreBackupCounts    map[string]disrupttest.WorkloadCounts `json:"preBackupCounts,omitempty"`
	FinalCounts        map[string]disrupttest.WorkloadCounts `json:"finalCounts,omitempty"`
	Scopes             []disrupttest.TestScope               `json:"scopes,omitempty"`
	BackupArtifacts    []BackupArtifact                      `json:"backupArtifacts,omitempty"`
	RecoveryDurationMS int64                                 `json:"recoveryDurationMs,omitempty"`
}

type BackupArtifact struct {
	PodName        string `json:"podName"`
	Ordinal        int32  `json:"ordinal"`
	ArchiveName    string `json:"archiveName"`
	ManifestName   string `json:"manifestName"`
	ChecksumSHA256 string `json:"checksumSha256"`
	LocalArchive   string `json:"localArchive"`
	LocalManifest  string `json:"localManifest"`
}

type runState struct {
	cfg           Config
	profile       Profile
	cluster       disrupttest.ClusterConfig
	driver        *disrupttest.K3SDriver
	runner        disrupttest.CommandRunner
	artifactRoot  string
	backupDir     string
	workload      disrupttest.Workload
	scopes        []disrupttest.TestScope
	expected      map[string]disrupttest.WorkloadCounts
	successful    int64
	attempted     int64
	eventFile     string
	readEventFile string
}

func NewHarness(cfg Config, runner disrupttest.CommandRunner) (*Harness, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if runner == nil {
		runner = disrupttest.ExecRunner{}
	}
	return &Harness{Config: cfg, Provisioner: disrupttest.NewK3DProvisioner(cfg.ClusterName, runner), Runner: runner}, nil
}

func (h *Harness) Run(ctx context.Context) (summary Summary, err error) {
	cfg := h.Config
	profile, err := ResolveProfile(cfg.Profile)
	if err != nil {
		return Summary{}, err
	}
	workload, err := disrupttest.ResolveWorkload(cfg.Workload)
	if err != nil {
		return Summary{}, err
	}
	if h.ArtifactRoot == "" {
		h.ArtifactRoot = filepath.Join(cfg.ArtifactsDir, time.Now().UTC().Format("20060102-150405")+"-"+cfg.ClusterName)
	}
	summary = Summary{Status: "PASS", ClusterName: cfg.ClusterName, Namespace: cfg.Namespace, Profile: profile.Name, Workload: workload.Name(), ArtifactDir: h.ArtifactRoot}
	if cfg.DryRun {
		return summary, nil
	}
	if err := os.MkdirAll(h.ArtifactRoot, 0o755); err != nil {
		return summary, err
	}
	clusterCfg, err := h.clusterConfig()
	if err != nil {
		return summary, err
	}
	r := &runState{cfg: cfg, profile: profile, cluster: clusterCfg, runner: h.Runner, artifactRoot: h.ArtifactRoot, backupDir: cfg.BackupDir, workload: workload, expected: map[string]disrupttest.WorkloadCounts{}, eventFile: filepath.Join(h.ArtifactRoot, "workload", "write-events.jsonl"), readEventFile: filepath.Join(h.ArtifactRoot, "workload", "read-events.jsonl")}
	defer func() {
		if err != nil {
			summary.Status = "FAIL"
			summary.Error = summarize(err.Error(), 500)
			_ = os.WriteFile(filepath.Join(h.ArtifactRoot, "error.txt"), []byte(err.Error()+"\n"), 0o644)
		}
		_ = writeJSON(filepath.Join(h.ArtifactRoot, "result-summary.json"), summary)
	}()

	progressf("running preflight checks")
	if err := h.Provisioner.Preflight(ctx); err != nil {
		return summary, err
	}
	progressf("creating disposable %s cluster %s", h.Provisioner.Name(), clusterCfg.Name)
	kubeCtx, err := h.Provisioner.Create(ctx, clusterCfg)
	if err != nil {
		return summary, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			if cleanupErr := h.Provisioner.Delete(context.Background()); cleanupErr != nil && err == nil {
				err = cleanupErr
			}
		}
	}()
	driver := disrupttest.NewK3SDriver(kubeCtx.Name, cfg.Namespace, cfg.Selector, cfg.Service, cfg.StatefulSet, h.Runner)
	h.Driver = driver
	r.driver = driver
	progressf("loading image into cluster image=%s", cfg.Image)
	if err := h.Provisioner.LoadImage(ctx, cfg.Image); err != nil {
		return summary, err
	}
	progressf("waiting for k3s system readiness")
	if err := driver.WaitSystemReady(ctx); err != nil {
		_ = driver.CollectArtifacts(ctx, filepath.Join(h.ArtifactRoot, "failure"))
		if cfg.KeepClusterOnFailure {
			cleanup = false
		}
		return summary, err
	}
	progressf("applying mycel manifests and waiting for pods")
	manifests, err := disrupttest.RenderManifests(disrupttest.ManifestConfigFromCluster(clusterCfg, cfg.StatefulSet, cfg.Service, cfg.Selector))
	if err != nil {
		return summary, err
	}
	if err := driver.ApplyManifests(ctx, manifests); err != nil {
		_ = driver.CollectArtifacts(ctx, filepath.Join(h.ArtifactRoot, "failure"))
		if cfg.KeepClusterOnFailure {
			cleanup = false
		}
		return summary, err
	}
	_ = driver.CollectArtifacts(ctx, filepath.Join(h.ArtifactRoot, "setup"))
	if cfg.SetupOnly {
		return summary, nil
	}

	result, err := r.runScenario(ctx)
	summary = result
	if err != nil {
		_ = driver.CollectArtifacts(ctx, filepath.Join(h.ArtifactRoot, "failure"))
		if cfg.KeepClusterOnFailure {
			cleanup = false
		}
		return summary, err
	}
	return summary, nil
}

func (h *Harness) clusterConfig() (disrupttest.ClusterConfig, error) {
	adminPassword, err := randomB64(24)
	if err != nil {
		return disrupttest.ClusterConfig{}, err
	}
	backendToken, err := randomB64(32)
	if err != nil {
		return disrupttest.ClusterConfig{}, err
	}
	encryptionKey, err := randomB64(32)
	if err != nil {
		return disrupttest.ClusterConfig{}, err
	}
	return disrupttest.ClusterConfig{Name: h.Config.ClusterName, Namespace: h.Config.Namespace, Image: h.Config.Image, AdminUsername: h.Config.AdminUsername, AdminPassword: adminPassword, BackendToken: backendToken, EncryptionKey: encryptionKey, NodeCount: h.Config.NodeCount, PartitionCount: h.Config.PartitionCount}, nil
}

func (r *runState) runScenario(ctx context.Context) (Summary, error) {
	summary := Summary{Status: "PASS", ClusterName: r.cfg.ClusterName, Namespace: r.cfg.Namespace, Profile: r.profile.Name, Workload: r.workload.Name(), ArtifactDir: r.artifactRoot}
	progressf("connecting workload endpoint")
	client, cleanup, err := r.connectService(ctx)
	if err != nil {
		return summary, err
	}
	defer cleanup()
	progressf("creating workload scope(s) workload=%s", r.workload.Name())
	r.scopes, err = r.workload.Setup(ctx, client, time.Now().UTC().Format("20060102-150405"))
	if err != nil {
		return summary, err
	}
	progressf("writing workload data writes=%d", r.profile.Writes)
	if err := r.writeWorkload(ctx, client); err != nil {
		return r.fillSummary(summary, nil, nil, nil, nil, ""), err
	}
	progressf("waiting for pre-backup count convergence")
	preCounts, err := r.waitCounts(ctx)
	if err != nil {
		return r.fillSummary(summary, preCounts, nil, nil, nil, ""), err
	}
	if r.cfg.NoBackup {
		return r.fillSummary(summary, preCounts, preCounts, nil, nil, ""), nil
	}
	progressf("capturing coordinated cluster system backup")
	backup, err := r.captureBackup(ctx)
	if err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, nil, ""), err
	}
	progressf("wiping namespace and deleting PVCs")
	oldUIDs, err := r.pvcUIDs(ctx)
	if err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, nil, backup.setID), err
	}
	if err := r.resetNamespace(ctx); err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, nil, backup.setID), err
	}
	progressf("creating fresh PVCs and restoring archives")
	if err := r.applyBaseResources(ctx); err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, nil, backup.setID), err
	}
	if err := r.createRestorePVCs(ctx); err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, nil, backup.setID), err
	}
	newUIDs, err := r.pvcUIDs(ctx)
	if err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, nil, backup.setID), err
	}
	if err := verifyPVCReplacement(oldUIDs, newUIDs); err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, newUIDs, backup.setID), err
	}
	if err := r.restoreArchives(ctx, backup.artifacts); err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, newUIDs, backup.setID), err
	}
	progressf("starting restored cluster")
	start := time.Now()
	if err := r.applyStatefulSet(ctx); err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, newUIDs, backup.setID), err
	}
	if err := r.driver.WaitAllReady(ctx); err != nil {
		return r.fillSummary(summary, preCounts, nil, backup, newUIDs, backup.setID), err
	}
	progressf("waiting for restored count convergence")
	finalCounts, err := r.waitCounts(ctx)
	if err != nil {
		return r.fillSummary(summary, preCounts, finalCounts, backup, newUIDs, backup.setID), err
	}
	if err := compareRestoredCounts(preCounts, finalCounts); err != nil {
		return r.fillSummary(summary, preCounts, finalCounts, backup, newUIDs, backup.setID), err
	}
	if err := r.verifyGQLRead(ctx); err != nil {
		return r.fillSummary(summary, preCounts, finalCounts, backup, newUIDs, backup.setID), err
	}
	summary = r.fillSummary(summary, preCounts, finalCounts, backup, newUIDs, backup.setID)
	summary.OldPVCUIDs = oldUIDs
	summary.RecoveryDurationMS = time.Since(start).Milliseconds()
	return summary, nil
}

func (r *runState) recordWriteEvent(ev disrupttest.WriteEvent) {
	r.appendJSONL(r.eventFile, ev)
}

func (r *runState) recordReadEvent(ev disrupttest.ReadEvent) {
	r.appendJSONL(r.readEventFile, ev)
}

func (r *runState) appendJSONL(path string, value any) {
	if strings.TrimSpace(path) == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(data, '\n'))
}

func (r *runState) writeWorkload(ctx context.Context, client *disrupttest.MycelClient) error {
	for i := int64(1); i <= r.profile.Writes; i++ {
		r.attempted++
		worker := "backup-writer"
		if err := r.workload.Write(ctx, client, r.scopes, worker, i); err != nil {
			r.recordWriteEvent(disrupttest.WriteEvent{Time: time.Now().UTC(), RunID: r.scopes[0].RunID, Worker: worker, Seq: i, Attempt: 1, Success: false, Transient: disrupttest.IsTransientError(err), Error: err.Error()})
			return fmt.Errorf("write workload seq %d: %w", i, err)
		}
		r.successful++
		r.recordWriteEvent(disrupttest.WriteEvent{Time: time.Now().UTC(), RunID: r.scopes[0].RunID, Worker: worker, Seq: i, Attempt: 1, Success: true})
		for scope, count := range r.workload.ExpectedWriteCounts(r.scopes, worker, i) {
			r.expected[scope] = r.expected[scope].Add(count)
		}
	}
	return nil
}

type backupResult struct {
	setID     string
	artifacts []BackupArtifact
}

func (r *runState) captureBackup(ctx context.Context) (*backupResult, error) {
	nodes, err := r.driver.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no pods available for backup")
	}
	for _, node := range nodes {
		if _, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "exec", node.Name, "--", "sh", "-ec", "rm -rf "+shellQuote(r.backupDir)+" && mkdir -p "+shellQuote(r.backupDir)); err != nil {
			return nil, err
		}
	}
	client, cleanup, err := r.connectPod(ctx, nodes[0])
	if err != nil {
		return nil, err
	}
	defer cleanup()
	res, err := client.TriggerClusterBackup(ctx, "workload-driven system backup/restore test", r.backupDir, r.cfg.ArchiveFormat)
	if err != nil {
		return nil, err
	}
	st := res.GetStatus()
	if strings.ToLower(st.GetState()) != "succeeded" {
		return nil, fmt.Errorf("cluster backup state %q phase=%s error=%s", st.GetState(), st.GetFailedPhase(), st.GetError())
	}
	if st.GetExpectedNodes() != int32(r.cfg.NodeCount) || len(st.GetNodes()) != r.cfg.NodeCount {
		return nil, fmt.Errorf("backup expected %d nodes, status expected=%d artifacts=%d", r.cfg.NodeCount, st.GetExpectedNodes(), len(st.GetNodes()))
	}
	if len(st.GetRaftBarriers()) == 0 {
		return nil, fmt.Errorf("backup status missing raft barrier evidence")
	}
	backupArtifactDir := filepath.Join(r.artifactRoot, "backup")
	if err := os.MkdirAll(backupArtifactDir, 0o755); err != nil {
		return nil, err
	}
	if err := r.copyFromPod(ctx, nodes[0].Name, filepath.Join(r.backupDir, "backup-set.json"), filepath.Join(backupArtifactDir, "backup-set.json")); err != nil {
		return nil, err
	}
	if err := validateCopiedBackupSet(filepath.Join(backupArtifactDir, "backup-set.json")); err != nil {
		return nil, err
	}
	var artifacts []BackupArtifact
	coordinatorPod := nodes[0].Name
	for _, node := range st.GetNodes() {
		pod := node.GetPodName()
		podDir := filepath.Join(backupArtifactDir, pod)
		if err := os.MkdirAll(podDir, 0o755); err != nil {
			return nil, err
		}
		archiveDest := filepath.Join(podDir, node.GetArchiveName())
		manifestDest := filepath.Join(podDir, node.GetManifestName())
		if err := r.copyFromPod(ctx, pod, filepath.Join(r.backupDir, node.GetArchiveName()), archiveDest); err != nil {
			return nil, err
		}
		if err := verifySHA256(archiveDest, node.GetChecksumSha256()); err != nil {
			return nil, err
		}
		if node.GetManifestName() != "" {
			if err := r.copyFromPod(ctx, pod, filepath.Join(r.backupDir, node.GetManifestName()), manifestDest); err != nil {
				return nil, err
			}
		}
		if pod != coordinatorPod {
			if _, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "cp", archiveDest, coordinatorPod+":"+filepath.Join(r.backupDir, node.GetArchiveName())); err != nil {
				return nil, err
			}
			if node.GetManifestName() != "" {
				if _, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "cp", manifestDest, coordinatorPod+":"+filepath.Join(r.backupDir, node.GetManifestName())); err != nil {
					return nil, err
				}
			}
		}
		artifacts = append(artifacts, BackupArtifact{PodName: pod, Ordinal: node.GetOrdinal(), ArchiveName: node.GetArchiveName(), ManifestName: node.GetManifestName(), ChecksumSHA256: node.GetChecksumSha256(), LocalArchive: archiveDest, LocalManifest: manifestDest})
	}
	if valid, err := client.ValidateClusterBackupSet(ctx, r.backupDir); err != nil {
		return nil, err
	} else if !valid.GetValid() {
		return nil, fmt.Errorf("cluster backup set invalid: %s", strings.Join(valid.GetErrors(), "; "))
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Ordinal < artifacts[j].Ordinal })
	return &backupResult{setID: st.GetBackupSetId(), artifacts: artifacts}, nil
}

func (r *runState) waitCounts(ctx context.Context) (map[string]disrupttest.WorkloadCounts, error) {
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		counts, err := r.collectCounts(ctx)
		if err != nil {
			lastErr = err
		} else if err := disrupttest.AssertCountsConverged(counts, r.workload.ExpectedMinimum(r.successful), r.expected); err != nil {
			lastErr = err
		} else {
			return counts, nil
		}
		select {
		case <-ctx.Done():
			return counts, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("counts did not converge: %w", lastErr)
}

func (r *runState) collectCounts(ctx context.Context) (map[string]disrupttest.WorkloadCounts, error) {
	nodes, err := r.driver.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	counts := map[string]disrupttest.WorkloadCounts{}
	for _, node := range nodes {
		client, cleanup, err := r.connectPod(ctx, node)
		if err != nil {
			return nil, fmt.Errorf("pod %s connect: %w", node.Name, err)
		}
		count, countErr := client.LocalConsistencyCounts(ctx, r.scopes)
		_ = client.Close()
		cleanup()
		if countErr != nil {
			return nil, fmt.Errorf("pod %s count: %w", node.Name, countErr)
		}
		counts[node.Name] = count
		if _, ok := counts["client"]; !ok {
			counts["client"] = count
		}
	}
	return counts, nil
}

func (r *runState) verifyGQLRead(ctx context.Context) error {
	nodes, err := r.driver.Nodes(ctx)
	if err != nil {
		return err
	}
	var failures []string
	for _, node := range nodes {
		client, cleanup, err := r.connectPod(ctx, node)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s connect: %v", node.Name, err))
			r.recordReadEvent(disrupttest.ReadEvent{Time: time.Now().UTC(), RunID: r.scopes[0].RunID, Worker: node.Name, Success: false, Transient: disrupttest.IsTransientError(err), Error: err.Error()})
			continue
		}
		_, countErr := r.workload.Count(ctx, client, r.scopes)
		_ = client.Close()
		cleanup()
		if countErr != nil {
			failures = append(failures, fmt.Sprintf("%s count: %v", node.Name, countErr))
			r.recordReadEvent(disrupttest.ReadEvent{Time: time.Now().UTC(), RunID: r.scopes[0].RunID, Worker: node.Name, Success: false, Transient: disrupttest.IsTransientError(countErr), Error: countErr.Error()})
			continue
		}
		r.recordReadEvent(disrupttest.ReadEvent{Time: time.Now().UTC(), RunID: r.scopes[0].RunID, Worker: node.Name, Success: true})
		if len(failures) > 0 {
			progressf("restored workload GQL verification used session-capable pod %s; other pod session attempts failed: %s", node.Name, strings.Join(failures, "; "))
		}
		return nil
	}
	return fmt.Errorf("restored workload GQL verification failed on all pods: %s", strings.Join(failures, "; "))
}

func (r *runState) resetNamespace(ctx context.Context) error {
	_, _ = r.kubectl(ctx, "delete", "namespace", r.cfg.Namespace, "--wait=true", "--timeout=5m")
	_, err := r.kubectl(ctx, "create", "namespace", r.cfg.Namespace)
	return err
}

func (r *runState) applyBaseResources(ctx context.Context) error {
	manifest := renderBaseResources(disrupttest.ManifestConfigFromCluster(r.cluster, r.cfg.StatefulSet, r.cfg.Service, r.cfg.Selector))
	artifactPath := filepath.Join(r.artifactRoot, "restore", "base-resources.redacted.yaml")
	return r.applyYAML(ctx, manifest, artifactPath)
}

func (r *runState) applyStatefulSet(ctx context.Context) error {
	manifests, err := disrupttest.RenderManifests(disrupttest.ManifestConfigFromCluster(r.cluster, r.cfg.StatefulSet, r.cfg.Service, r.cfg.Selector))
	if err != nil {
		return err
	}
	artifactPath := filepath.Join(r.artifactRoot, "restore", "statefulset.redacted.yaml")
	return r.applyYAML(ctx, manifests, artifactPath)
}

func (r *runState) applyYAML(ctx context.Context, manifest, redactedArtifactPath string) error {
	if strings.TrimSpace(redactedArtifactPath) != "" {
		if err := os.MkdirAll(filepath.Dir(redactedArtifactPath), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(redactedArtifactPath, []byte(redactManifest(manifest)), 0o644); err != nil {
			return err
		}
	}
	tmp, err := os.CreateTemp("", "mycel-system-backuptest-*.yaml")
	if err != nil {
		return err
	}
	path := tmp.Name()
	defer os.Remove(path)
	if _, err := tmp.WriteString(manifest); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	_, err = r.kubectl(ctx, "apply", "-f", path)
	return err
}

func redactManifest(manifest string) string {
	lines := strings.Split(manifest, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		for _, key := range []string{"bootstrap-admin-password:", "user-store-encryption-key-b64:", "cluster-backend-auth-token:"} {
			if strings.HasPrefix(trimmed, key) {
				indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
				lines[i] = indent + key + " <redacted>"
			}
		}
	}
	return strings.Join(lines, "\n")
}

func renderBaseResources(cfg disrupttest.ManifestConfig) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
---
apiVersion: v1
kind: Secret
metadata:
  name: myceld-secret
  namespace: %s
type: Opaque
stringData:
  bootstrap-admin-username: %q
  bootstrap-admin-password: %q
  user-store-encryption-key-b64: %q
  cluster-backend-auth-token: %q
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: myceld-config
  namespace: %s
data:
  MYCELD_MODE: standalone
  MYCELD_GRPC_ADDR: 0.0.0.0:9091
  MYCELD_CLUSTER_NAME: %q
  MYCELD_CLUSTER_RAFT_NODE_COUNT: %q
  MYCELD_CLUSTER_RAFT_REPLICA_FACTOR: %q
  MYCELD_CLUSTER_RAFT_PARTITION_COUNT: %q
  MYCELD_CLUSTER_RAFT_NODE_ADDRS: %q
---
apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: %s
  ports:
    - name: grpc
      port: 9091
      targetPort: 9091
---
apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
spec:
  selector:
    app: %s
  ports:
    - name: grpc
      port: 9091
      targetPort: 9091
`, cfg.Namespace, cfg.Namespace, cfg.AdminUsername, cfg.AdminPassword, cfg.EncryptionKey, cfg.BackendToken, cfg.Namespace, cfg.ClusterName, fmt.Sprint(cfg.NodeCount), fmt.Sprint(cfg.NodeCount), fmt.Sprint(cfg.PartitionCount), cfg.NodeAddrs, cfg.HeadlessService, cfg.Namespace, cfg.SelectorApp, cfg.Service, cfg.Namespace, cfg.SelectorApp)
}

func (r *runState) createRestorePVCs(ctx context.Context) error {
	var b strings.Builder
	for i := 0; i < r.cfg.NodeCount; i++ {
		fmt.Fprintf(&b, `apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: myceld-data-%s-%d
  namespace: %s
  labels:
    app: %s
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
---
`, r.cfg.StatefulSet, i, r.cfg.Namespace, selectorApp(r.cfg.Selector, r.cfg.StatefulSet))
	}
	path := filepath.Join(r.artifactRoot, "restore", "pvcs.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return err
	}
	_, err := r.kubectl(ctx, "apply", "-f", path)
	return err
}

func (r *runState) restoreArchives(ctx context.Context, artifacts []BackupArtifact) error {
	for _, artifact := range artifacts {
		restorePod := "restore-" + artifact.PodName
		pvc := "myceld-data-" + artifact.PodName
		manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: restore
      image: alpine:3.21
      command: ["/bin/sh", "-ec", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data/mycel
        - name: restore
          mountPath: /restore
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s
    - name: restore
      emptyDir: {}
`, restorePod, r.cfg.Namespace, pvc)
		path := filepath.Join(r.artifactRoot, "restore", restorePod+".yaml")
		if err := os.WriteFile(path, []byte(manifest), 0o644); err != nil {
			return err
		}
		if _, err := r.kubectl(ctx, "apply", "-f", path); err != nil {
			return err
		}
		if _, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "wait", "--for=condition=Ready", "pod/"+restorePod, "--timeout=5m"); err != nil {
			return err
		}
		if _, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "cp", artifact.LocalArchive, restorePod+":/restore/backup.tar"); err != nil {
			return err
		}
		if _, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "exec", restorePod, "--", "sh", "-ec", "find /data/mycel -mindepth 1 -maxdepth 1 -exec rm -rf {} + && tar -xf /restore/backup.tar -C /data/mycel"); err != nil {
			return err
		}
		if _, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "delete", "pod", restorePod, "--wait=true", "--timeout=3m"); err != nil {
			return err
		}
	}
	return nil
}

func (r *runState) pvcUIDs(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	for i := 0; i < r.cfg.NodeCount; i++ {
		name := fmt.Sprintf("myceld-data-%s-%d", r.cfg.StatefulSet, i)
		res, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "get", "pvc", name, "-o", "jsonpath={.metadata.uid}")
		if err != nil {
			return nil, err
		}
		out[name] = strings.TrimSpace(res.Stdout)
	}
	return out, nil
}

func verifyPVCReplacement(oldUIDs, newUIDs map[string]string) error {
	if len(oldUIDs) == 0 || len(newUIDs) == 0 {
		return fmt.Errorf("missing PVC UID evidence old=%d new=%d", len(oldUIDs), len(newUIDs))
	}
	for name, oldUID := range oldUIDs {
		newUID := newUIDs[name]
		if strings.TrimSpace(oldUID) == "" || strings.TrimSpace(newUID) == "" {
			return fmt.Errorf("missing PVC UID for %s old=%q new=%q", name, oldUID, newUID)
		}
		if oldUID == newUID {
			return fmt.Errorf("PVC %s was not replaced; UID remained %s", name, oldUID)
		}
	}
	return nil
}

func compareRestoredCounts(pre, post map[string]disrupttest.WorkloadCounts) error {
	if len(pre) == 0 || len(post) == 0 {
		return fmt.Errorf("missing count evidence pre=%d post=%d", len(pre), len(post))
	}
	want := pre["client"]
	for name, got := range post {
		if !got.Equal(want) {
			return fmt.Errorf("restored %s count %+v differs from pre-backup %+v", name, got, want)
		}
	}
	return nil
}

func (r *runState) fillSummary(summary Summary, preCounts, finalCounts map[string]disrupttest.WorkloadCounts, backup *backupResult, newUIDs map[string]string, backupSetID string) Summary {
	summary.AttemptedWrites = r.attempted
	summary.SuccessfulWrites = r.successful
	summary.PreBackupCounts = preCounts
	summary.FinalCounts = finalCounts
	summary.Scopes = r.scopes
	if finalCounts != nil {
		summary.AmbiguousWrites = disrupttest.EstimateAmbiguousWrites(r.workload.Name(), finalCounts, r.successful)
	}
	if backup != nil {
		summary.BackupSetID = backup.setID
		summary.BackupArtifacts = backup.artifacts
		summary.PVCsRestored = len(backup.artifacts)
	}
	if backupSetID != "" {
		summary.BackupSetID = backupSetID
	}
	if newUIDs != nil {
		summary.NewPVCUIDs = newUIDs
		summary.PVCsDeleted = len(newUIDs)
	}
	return summary
}

func (r *runState) connectService(ctx context.Context) (*disrupttest.MycelClient, func(), error) {
	endpoint, cleanup, err := r.driver.ServiceEndpoint(ctx)
	if err != nil {
		return nil, nil, err
	}
	client, err := disrupttest.DialMycel(ctx, endpoint, r.cfg.AdminUsername, r.cluster.AdminPassword)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return client, func() { _ = client.Close(); cleanup() }, nil
}

func (r *runState) connectPod(ctx context.Context, node disrupttest.NodeRef) (*disrupttest.MycelClient, func(), error) {
	endpoint, cleanup, err := r.driver.PortForward(ctx, node, 9091)
	if err != nil {
		return nil, nil, err
	}
	client, err := disrupttest.DialMycel(ctx, endpoint, r.cfg.AdminUsername, r.cluster.AdminPassword)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return client, func() { _ = client.Close(); cleanup() }, nil
}

func (r *runState) copyFromPod(ctx context.Context, pod, remote, local string) error {
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return err
	}
	_, err := r.kubectl(ctx, "-n", r.cfg.Namespace, "cp", pod+":"+remote, local)
	return err
}

func (r *runState) kubectl(ctx context.Context, args ...string) (disrupttest.CommandResult, error) {
	base := []string{}
	if r.driver.KubeContext != "" {
		base = append(base, "--context", r.driver.KubeContext)
	}
	base = append(base, args...)
	return r.runner.Run(ctx, "kubectl", base...)
}

func validateCopiedBackupSet(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if containsForbiddenBackupMaterial(string(data)) {
		return fmt.Errorf("backup-set metadata contains forbidden secret/session material")
	}
	manifest, err := backupcluster.Parse(data)
	if err != nil {
		return err
	}
	if err := backupcluster.Validate(manifest, backupcluster.ValidationModeRestore); err != nil {
		return err
	}
	if len(manifest.RaftBarriers) == 0 {
		return fmt.Errorf("backup-set manifest missing raft barriers")
	}
	return nil
}

func containsForbiddenBackupMaterial(text string) bool {
	lower := strings.ToLower(text)
	for _, needle := range []string{"access_token", "refresh_token", "session_token", "password\":", "bootstrap-admin-password", "authorization"} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func verifySHA256(path, want string) error {
	if strings.TrimSpace(want) == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got != strings.TrimSpace(want) {
		return fmt.Errorf("sha256 mismatch for %s: got %s want %s", path, got, want)
	}
	return nil
}

func randomB64(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

func selectorApp(selector, fallback string) string {
	_, value, ok := strings.Cut(selector, "=")
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func summarize(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	if limit > 0 && len(text) > limit {
		return text[:limit] + "..."
	}
	return text
}

func progressf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[system-backup] "+format+"\n", args...)
}

func writeJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
