package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	clusterCaptureProviderFilesystem = "filesystem_immutable_copy"
	clusterCaptureProviderCSI        = "csi_volume_snapshot"
	clusterCaptureProviderAWSEBS     = "aws_ebs_snapshot"

	manifestCaptureProviderFilesystem = "FILESYSTEM_IMMUTABLE_COPY"
	manifestCaptureProviderCSI        = "CSI_VOLUME_SNAPSHOT"
	manifestCaptureProviderAWSEBS     = "AWS_EBS_SNAPSHOT"

	defaultKubernetesSnapshotSourcePVCTemplate = "myceld-data-{pod}"
)

// ClusterSnapshotCaptureProvider captures immutable volume snapshots for a
// cluster backup. Implementations must not publish raw credentials or provider
// secrets in returned evidence.
type ClusterSnapshotCaptureProvider interface {
	CaptureClusterSnapshot(context.Context, ClusterSnapshotCaptureRequest) (ClusterSnapshotCaptureResult, error)
	CleanupClusterSnapshot(context.Context, ClusterSnapshotCaptureResult) ClusterSnapshotCleanupResult
}

type ClusterSnapshotCaptureRequest struct {
	BackupSetID     string
	ClusterID       string
	Namespace       string
	StatefulSet     string
	CaptureProvider string
	CreatedAt       time.Time
	Nodes           []ClusterBackupNode
}

type ClusterSnapshotCaptureResult struct {
	Provider          string
	ProviderVersion   string
	FallbackUsed      bool
	SnapshotClassName string
	DataMover         string
	Evidence          json.RawMessage
	NodeSources       map[string]json.RawMessage
	Cleanup           ClusterSnapshotCleanupResult
}

type ClusterSnapshotCleanupResult struct {
	Attempted   bool                         `json:"attempted"`
	Completed   bool                         `json:"completed"`
	Retained    bool                         `json:"retained"`
	StartedAt   time.Time                    `json:"started_at,omitempty"`
	CompletedAt time.Time                    `json:"completed_at,omitempty"`
	Items       []ClusterSnapshotCleanupItem `json:"items,omitempty"`
	Warnings    []string                     `json:"warnings,omitempty"`
	Evidence    map[string]string            `json:"evidence,omitempty"`
}

type ClusterSnapshotCleanupItem struct {
	PodName     string `json:"pod_name,omitempty"`
	ResourceURI string `json:"resource_uri"`
	State       string `json:"state"`
	LastError   string `json:"last_error,omitempty"`
}

type clusterBackupCaptureEvidence struct {
	Provider          string                       `json:"provider,omitempty"`
	ProviderVersion   string                       `json:"provider_version,omitempty"`
	FallbackUsed      bool                         `json:"fallback_used"`
	SnapshotClassName string                       `json:"snapshot_class_name,omitempty"`
	DataMover         string                       `json:"data_mover,omitempty"`
	ProviderEvidence  json.RawMessage              `json:"provider_evidence,omitempty"`
	NodeSources       map[string]json.RawMessage   `json:"node_sources,omitempty"`
	Cleanup           ClusterSnapshotCleanupResult `json:"cleanup,omitempty"`
}

type KubernetesSnapshotCaptureProviderConfig struct {
	Namespace          string
	SnapshotClassName  string
	SourcePVCTemplate  string
	DeleteOnCompletion bool
	Timeout            time.Duration
	PollInterval       time.Duration
	AWSDriverHint      string
}

type KubernetesSnapshotCaptureProvider struct {
	config KubernetesSnapshotCaptureProviderConfig
	api    *KubernetesPVCRestoreHelper
}

func NewInClusterKubernetesSnapshotCaptureProvider(config KubernetesSnapshotCaptureProviderConfig) (*KubernetesSnapshotCaptureProvider, error) {
	api, err := NewInClusterKubernetesPVCRestoreHelper(KubernetesPVCRestoreHelperConfig{Namespace: config.Namespace})
	if err != nil {
		return nil, err
	}
	config.Namespace = api.config.Namespace
	return newKubernetesSnapshotCaptureProviderFromAPI(api, config), nil
}

func newKubernetesSnapshotCaptureProvider(baseURL, token string, client *http.Client, config KubernetesSnapshotCaptureProviderConfig) *KubernetesSnapshotCaptureProvider {
	return newKubernetesSnapshotCaptureProviderFromAPI(newKubernetesPVCRestoreHelper(baseURL, token, client, KubernetesPVCRestoreHelperConfig{Namespace: config.Namespace}), config)
}

func newKubernetesSnapshotCaptureProviderFromAPI(api *KubernetesPVCRestoreHelper, config KubernetesSnapshotCaptureProviderConfig) *KubernetesSnapshotCaptureProvider {
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Minute
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 2 * time.Second
	}
	if strings.TrimSpace(config.SourcePVCTemplate) == "" {
		config.SourcePVCTemplate = defaultKubernetesSnapshotSourcePVCTemplate
	}
	if strings.TrimSpace(config.AWSDriverHint) == "" {
		config.AWSDriverHint = "ebs.csi.aws.com"
	}
	return &KubernetesSnapshotCaptureProvider{api: api, config: config}
}

func (p *KubernetesSnapshotCaptureProvider) CaptureClusterSnapshot(ctx context.Context, req ClusterSnapshotCaptureRequest) (ClusterSnapshotCaptureResult, error) {
	if p == nil {
		return ClusterSnapshotCaptureResult{}, fmt.Errorf("Kubernetes snapshot capture provider is not configured")
	}
	provider := normalizeClusterCaptureProvider(req.CaptureProvider)
	if provider != clusterCaptureProviderCSI && provider != clusterCaptureProviderAWSEBS {
		return ClusterSnapshotCaptureResult{}, fmt.Errorf("Kubernetes snapshot capture provider does not support capture_provider %q", req.CaptureProvider)
	}
	if strings.TrimSpace(req.BackupSetID) == "" {
		return ClusterSnapshotCaptureResult{}, fmt.Errorf("backup_set_id is required")
	}
	if len(req.Nodes) == 0 {
		return ClusterSnapshotCaptureResult{}, fmt.Errorf("snapshot capture nodes are required")
	}
	namespace := firstNonEmptyString(req.Namespace, p.config.Namespace, "default")
	startedAt := time.Now().UTC()
	if err := p.preflight(ctx, namespace); err != nil {
		return ClusterSnapshotCaptureResult{}, err
	}
	created := make([]kubernetesSnapshotNodeEvidence, 0, len(req.Nodes))
	defer func() {
		if len(created) == 0 {
			return
		}
		// Capture failures must not leave partially-created snapshots behind. Best
		// effort cleanup is intentionally not returned here because the capture
		// operation itself will fail closed and no COMPLETE marker will be published.
		_ = p.cleanupSnapshots(context.Background(), namespace, created)
	}()
	for _, node := range req.Nodes {
		if strings.TrimSpace(node.PodName) == "" {
			return ClusterSnapshotCaptureResult{}, fmt.Errorf("snapshot capture node pod_name is required")
		}
		sourcePVC := kubernetesSnapshotSourcePVCName(p.config.SourcePVCTemplate, req.StatefulSet, node)
		if sourcePVC == "" {
			return ClusterSnapshotCaptureResult{}, fmt.Errorf("source PVC name for pod %s is empty", node.PodName)
		}
		snapshotName := kubernetesSnapshotName(req.BackupSetID, node.PodName, node.Ordinal)
		if err := p.createVolumeSnapshot(ctx, namespace, snapshotName, sourcePVC, req.BackupSetID, node); err != nil {
			return ClusterSnapshotCaptureResult{}, err
		}
		evidence := kubernetesSnapshotNodeEvidence{PodName: node.PodName, NodeID: node.NodeID, Ordinal: node.Ordinal, RaftNodeID: node.RaftNodeID, SourcePVCRef: "pvc/" + sourcePVC, SnapshotRef: "volumesnapshot.snapshot.storage.k8s.io/" + snapshotName, SnapshotName: snapshotName, Namespace: namespace, CreatedAt: time.Now().UTC()}
		created = append(created, evidence)
	}
	ready := make([]kubernetesSnapshotNodeEvidence, 0, len(created))
	for _, snap := range created {
		updated, err := p.waitForSnapshotReady(ctx, namespace, snap)
		if err != nil {
			return ClusterSnapshotCaptureResult{}, err
		}
		ready = append(ready, updated)
	}
	created = nil
	result := p.buildResult(req, namespace, startedAt, ready)
	return result, nil
}

func (p *KubernetesSnapshotCaptureProvider) CleanupClusterSnapshot(ctx context.Context, result ClusterSnapshotCaptureResult) ClusterSnapshotCleanupResult {
	if p == nil {
		return ClusterSnapshotCleanupResult{Attempted: false, Completed: false, Warnings: []string{"Kubernetes snapshot capture provider is not configured"}}
	}
	nodes := snapshotNodesFromResult(result)
	namespace := snapshotNamespaceFromResult(result)
	if namespace == "" {
		namespace = firstNonEmptyString(p.config.Namespace, "default")
	}
	if !p.config.DeleteOnCompletion {
		items := make([]ClusterSnapshotCleanupItem, 0, len(nodes))
		for _, node := range nodes {
			items = append(items, ClusterSnapshotCleanupItem{PodName: node.PodName, ResourceURI: "k8s://" + namespace + "/volumesnapshot/" + node.SnapshotName, State: "retained"})
		}
		return ClusterSnapshotCleanupResult{Attempted: false, Completed: true, Retained: true, CompletedAt: time.Now().UTC(), Items: items, Evidence: map[string]string{"delete_on_completion": "false"}}
	}
	return p.cleanupSnapshots(ctx, namespace, nodes)
}

func (p *KubernetesSnapshotCaptureProvider) preflight(ctx context.Context, namespace string) error {
	if _, _, _, err := p.api.doJSONStatus(ctx, http.MethodGet, fmt.Sprintf("/apis/snapshot.storage.k8s.io/v1/namespaces/%s/volumesnapshots?limit=1", url.PathEscape(namespace)), "", nil); err != nil {
		return fmt.Errorf("preflight Kubernetes CSI VolumeSnapshot API: %w", err)
	}
	if class := strings.TrimSpace(p.config.SnapshotClassName); class != "" {
		if _, _, _, err := p.api.doJSONStatus(ctx, http.MethodGet, "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses/"+url.PathEscape(class), "", nil); err != nil {
			return fmt.Errorf("preflight Kubernetes VolumeSnapshotClass %s: %w", class, err)
		}
	}
	return nil
}

func (p *KubernetesSnapshotCaptureProvider) createVolumeSnapshot(ctx context.Context, namespace, snapshotName, sourcePVC, backupSetID string, node ClusterBackupNode) error {
	spec := map[string]any{"source": map[string]any{"persistentVolumeClaimName": sourcePVC}}
	if class := strings.TrimSpace(p.config.SnapshotClassName); class != "" {
		spec["volumeSnapshotClassName"] = class
	}
	body := map[string]any{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata": map[string]any{
			"name":      snapshotName,
			"namespace": namespace,
			"labels": map[string]string{
				"app.kubernetes.io/name":       "mycel-snapshot-capture",
				"app.kubernetes.io/managed-by": "mycel-daemon",
				"myceldb.io/backup-set-id":     kubernetesLabelValue(backupSetID),
				"myceldb.io/source-pod":        kubernetesLabelValue(node.PodName),
			},
		},
		"spec": spec,
	}
	_, _, raw, err := p.api.doJSONStatus(ctx, http.MethodPost, fmt.Sprintf("/apis/snapshot.storage.k8s.io/v1/namespaces/%s/volumesnapshots", url.PathEscape(namespace)), "application/json", body)
	if err != nil {
		return fmt.Errorf("create VolumeSnapshot %s/%s for PVC %s: %w%s", namespace, snapshotName, sourcePVC, err, kubernetesRawSuffix(raw))
	}
	return nil
}

func (p *KubernetesSnapshotCaptureProvider) waitForSnapshotReady(ctx context.Context, namespace string, snap kubernetesSnapshotNodeEvidence) (kubernetesSnapshotNodeEvidence, error) {
	deadline := time.Now().Add(p.config.Timeout)
	for {
		if err := ctx.Err(); err != nil {
			return snap, err
		}
		raw, _, rawBody, err := p.api.doJSONStatus(ctx, http.MethodGet, fmt.Sprintf("/apis/snapshot.storage.k8s.io/v1/namespaces/%s/volumesnapshots/%s", url.PathEscape(namespace), url.PathEscape(snap.SnapshotName)), "", nil)
		if err != nil {
			return snap, fmt.Errorf("read VolumeSnapshot %s/%s: %w%s", namespace, snap.SnapshotName, err, kubernetesRawSuffix(rawBody))
		}
		ready, readySet := nestedBool(raw, "status", "readyToUse")
		if readySet {
			snap.ReadyToUse = ready
		}
		snap.SnapshotContentName = nestedString(raw, "status", "boundVolumeSnapshotContentName")
		snap.RestoreSize = nestedString(raw, "status", "restoreSize")
		if creationTime := nestedString(raw, "status", "creationTime"); creationTime != "" {
			if parsed, err := time.Parse(time.RFC3339, creationTime); err == nil {
				snap.ProviderCreationTime = parsed.UTC()
			}
		}
		if ready {
			snap.ReadyAt = time.Now().UTC()
			return snap, nil
		}
		if time.Now().After(deadline) {
			return snap, fmt.Errorf("VolumeSnapshot %s/%s was not ready before timeout; readyToUse=%v", namespace, snap.SnapshotName, ready)
		}
		timer := time.NewTimer(p.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return snap, ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *KubernetesSnapshotCaptureProvider) cleanupSnapshots(ctx context.Context, namespace string, nodes []kubernetesSnapshotNodeEvidence) ClusterSnapshotCleanupResult {
	result := ClusterSnapshotCleanupResult{Attempted: true, StartedAt: time.Now().UTC(), Evidence: map[string]string{"delete_on_completion": "true"}}
	completed := true
	for _, node := range nodes {
		uri := "k8s://" + namespace + "/volumesnapshot/" + node.SnapshotName
		state := "deleted"
		lastErr := ""
		if strings.TrimSpace(node.SnapshotName) == "" {
			state = "skipped"
		} else {
			_, statusCode, raw, err := p.api.doJSONStatus(ctx, http.MethodDelete, fmt.Sprintf("/apis/snapshot.storage.k8s.io/v1/namespaces/%s/volumesnapshots/%s", url.PathEscape(namespace), url.PathEscape(node.SnapshotName)), "application/json", map[string]any{"kind": "DeleteOptions", "apiVersion": "v1"})
			if err != nil && statusCode != http.StatusNotFound {
				completed = false
				state = "failed"
				lastErr = err.Error() + kubernetesRawSuffix(raw)
				result.Warnings = append(result.Warnings, fmt.Sprintf("delete VolumeSnapshot %s/%s: %v", namespace, node.SnapshotName, err))
			}
		}
		result.Items = append(result.Items, ClusterSnapshotCleanupItem{PodName: node.PodName, ResourceURI: uri, State: state, LastError: lastErr})
	}
	result.Completed = completed
	result.CompletedAt = time.Now().UTC()
	return result
}

func (p *KubernetesSnapshotCaptureProvider) buildResult(req ClusterSnapshotCaptureRequest, namespace string, startedAt time.Time, nodes []kubernetesSnapshotNodeEvidence) ClusterSnapshotCaptureResult {
	provider := normalizeClusterCaptureProvider(req.CaptureProvider)
	manifestProvider := productionCaptureProvider(provider)
	evidence := kubernetesSnapshotEvidenceDocument{
		Schema:            "mycel.kubernetes_snapshot_capture_evidence",
		Version:           1,
		Provider:          manifestProvider,
		ProviderVersion:   "snapshot.storage.k8s.io/v1",
		SnapshotClassName: strings.TrimSpace(p.config.SnapshotClassName),
		Namespace:         namespace,
		StatefulSet:       strings.TrimSpace(req.StatefulSet),
		SourcePVCTemplate: strings.TrimSpace(p.config.SourcePVCTemplate),
		DataMover:         snapshotDataMover(provider),
		StartedAt:         startedAt,
		CompletedAt:       time.Now().UTC(),
		Snapshots:         nodes,
	}
	if provider == clusterCaptureProviderAWSEBS {
		evidence.DriverHint = strings.TrimSpace(p.config.AWSDriverHint)
	}
	nodeSources := make(map[string]json.RawMessage, len(nodes))
	for _, node := range nodes {
		nodeSources[node.PodName] = mustJSONRaw(map[string]any{
			"provider":              manifestProvider,
			"source_pvc_ref":        node.SourcePVCRef,
			"snapshot_ref":          node.SnapshotRef,
			"snapshot_content_name": node.SnapshotContentName,
			"ready_to_use":          node.ReadyToUse,
			"captured_at":           node.ReadyAt,
		})
	}
	return ClusterSnapshotCaptureResult{Provider: provider, ProviderVersion: evidence.ProviderVersion, SnapshotClassName: evidence.SnapshotClassName, DataMover: evidence.DataMover, Evidence: mustJSONRaw(evidence), NodeSources: nodeSources}
}

type kubernetesSnapshotEvidenceDocument struct {
	Schema            string                           `json:"schema"`
	Version           int                              `json:"version"`
	Provider          string                           `json:"provider"`
	ProviderVersion   string                           `json:"provider_version"`
	SnapshotClassName string                           `json:"snapshot_class_name,omitempty"`
	Namespace         string                           `json:"namespace"`
	StatefulSet       string                           `json:"statefulset,omitempty"`
	SourcePVCTemplate string                           `json:"source_pvc_template"`
	DriverHint        string                           `json:"driver_hint,omitempty"`
	DataMover         string                           `json:"data_mover"`
	StartedAt         time.Time                        `json:"started_at"`
	CompletedAt       time.Time                        `json:"completed_at"`
	Snapshots         []kubernetesSnapshotNodeEvidence `json:"snapshots"`
	Cleanup           ClusterSnapshotCleanupResult     `json:"cleanup,omitempty"`
}

type kubernetesSnapshotNodeEvidence struct {
	PodName              string    `json:"pod_name"`
	NodeID               string    `json:"node_id,omitempty"`
	Ordinal              int       `json:"ordinal"`
	RaftNodeID           uint64    `json:"raft_node_id,omitempty"`
	Namespace            string    `json:"namespace"`
	SourcePVCRef         string    `json:"source_pvc_ref"`
	SnapshotRef          string    `json:"snapshot_ref"`
	SnapshotName         string    `json:"snapshot_name"`
	SnapshotContentName  string    `json:"snapshot_content_name,omitempty"`
	ReadyToUse           bool      `json:"ready_to_use"`
	RestoreSize          string    `json:"restore_size,omitempty"`
	ProviderCreationTime time.Time `json:"provider_creation_time,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
	ReadyAt              time.Time `json:"ready_at,omitempty"`
}

func snapshotNodesFromResult(result ClusterSnapshotCaptureResult) []kubernetesSnapshotNodeEvidence {
	var doc kubernetesSnapshotEvidenceDocument
	if len(result.Evidence) == 0 || json.Unmarshal(result.Evidence, &doc) != nil {
		return nil
	}
	return append([]kubernetesSnapshotNodeEvidence(nil), doc.Snapshots...)
}

func snapshotNamespaceFromResult(result ClusterSnapshotCaptureResult) string {
	var doc kubernetesSnapshotEvidenceDocument
	if len(result.Evidence) == 0 || json.Unmarshal(result.Evidence, &doc) != nil {
		return ""
	}
	return strings.TrimSpace(doc.Namespace)
}

func captureResultWithCleanup(result ClusterSnapshotCaptureResult, cleanup ClusterSnapshotCleanupResult) ClusterSnapshotCaptureResult {
	result.Cleanup = cleanup
	if len(result.Evidence) > 0 {
		var doc kubernetesSnapshotEvidenceDocument
		if json.Unmarshal(result.Evidence, &doc) == nil && doc.Schema == "mycel.kubernetes_snapshot_capture_evidence" {
			doc.Cleanup = cleanup
			result.Evidence = mustJSONRaw(doc)
		}
	}
	return result
}

func kubernetesSnapshotSourcePVCName(template, statefulSet string, node ClusterBackupNode) string {
	template = strings.TrimSpace(template)
	if template == "" {
		template = defaultKubernetesSnapshotSourcePVCTemplate
	}
	out := strings.ReplaceAll(template, "{pod}", strings.TrimSpace(node.PodName))
	out = strings.ReplaceAll(out, "{ordinal}", strconv.Itoa(node.Ordinal))
	out = strings.ReplaceAll(out, "{statefulset}", strings.TrimSpace(statefulSet))
	return kubernetesRestoreSafeName(out)
}

func kubernetesSnapshotName(backupSetID, podName string, ordinal int) string {
	suffix := hashString(strings.TrimSpace(backupSetID) + ":" + strings.TrimSpace(podName) + ":" + strconv.Itoa(ordinal))
	if len(suffix) > 10 {
		suffix = suffix[:10]
	}
	base := kubernetesRestoreSafeName("mycel-snap-" + strings.TrimSpace(backupSetID) + "-" + strconv.Itoa(ordinal))
	maxBase := 63 - len(suffix) - 1
	if maxBase < 1 {
		return "mycel-snap-" + suffix
	}
	if len(base) > maxBase {
		base = strings.Trim(base[:maxBase], "-")
	}
	if base == "" {
		base = "mycel-snap"
	}
	return base + "-" + suffix
}

func kubernetesLabelValue(value string) string {
	value = kubernetesRestoreSafeName(value)
	if len(value) > 63 {
		value = strings.Trim(value[:63], "-")
	}
	return value
}

func kubernetesRawSuffix(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return ": " + boundedString(raw, 4096)
}

func nestedBool(values map[string]any, path ...string) (bool, bool) {
	var current any = values
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return false, false
		}
		current = object[key]
	}
	value, ok := current.(bool)
	return value, ok
}

func normalizeClusterCaptureProvider(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	switch provider {
	case "", "auto":
		return clusterCaptureProviderFilesystem
	case clusterCaptureProviderFilesystem, clusterCaptureProviderCSI, clusterCaptureProviderAWSEBS:
		return provider
	default:
		return provider
	}
}

func productionCaptureProvider(provider string) string {
	switch normalizeClusterCaptureProvider(provider) {
	case clusterCaptureProviderCSI:
		return manifestCaptureProviderCSI
	case clusterCaptureProviderAWSEBS:
		return manifestCaptureProviderAWSEBS
	default:
		return manifestCaptureProviderFilesystem
	}
}

func snapshotDataMover(provider string) string {
	switch normalizeClusterCaptureProvider(provider) {
	case clusterCaptureProviderCSI:
		return "kubernetes-csi-volumesnapshot-plus-mycel-daemon-s3-compatible"
	case clusterCaptureProviderAWSEBS:
		return "aws-ebs-csi-volumesnapshot-plus-mycel-daemon-s3-compatible"
	default:
		return "mycel-daemon-s3-compatible"
	}
}

func captureProviderRequiresSnapshot(provider string) bool {
	switch normalizeClusterCaptureProvider(provider) {
	case clusterCaptureProviderCSI, clusterCaptureProviderAWSEBS:
		return true
	default:
		return false
	}
}
