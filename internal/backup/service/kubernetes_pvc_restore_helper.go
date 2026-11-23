package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultKubernetesServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	defaultKubernetesRestoreDataDir    = "/data/mycel"
)

type KubernetesPVCRestoreHelperConfig struct {
	Namespace            string
	HelperImage          string
	ServiceAccountName   string
	ImagePullPolicy      string
	Timeout              time.Duration
	PollInterval         time.Duration
	DeleteOnCompletion   bool
	ScaleDownStatefulSet bool
	ProvisionPVCs        bool
	ReplaceExistingPVCs  bool
	PVCStorageClassName  string
	PVCStorageRequest    string
	PVCAccessModes       []string
}

type KubernetesPVCRestoreHelper struct {
	config  KubernetesPVCRestoreHelperConfig
	baseURL string
	token   string
	client  *http.Client
}

func NewInClusterKubernetesPVCRestoreHelper(config KubernetesPVCRestoreHelperConfig) (*KubernetesPVCRestoreHelper, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, fmt.Errorf("in-cluster Kubernetes service environment is not available")
	}
	tokenRaw, err := os.ReadFile(filepath.Join(defaultKubernetesServiceAccountDir, "token"))
	if err != nil {
		return nil, fmt.Errorf("read Kubernetes service account token: %w", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	caRaw, err := os.ReadFile(filepath.Join(defaultKubernetesServiceAccountDir, "ca.crt"))
	if err == nil && len(caRaw) > 0 {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(caRaw) {
			client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
		}
	}
	if strings.TrimSpace(config.Namespace) == "" {
		if nsRaw, nsErr := os.ReadFile(filepath.Join(defaultKubernetesServiceAccountDir, "namespace")); nsErr == nil {
			config.Namespace = strings.TrimSpace(string(nsRaw))
		}
	}
	if strings.TrimSpace(config.Namespace) == "" {
		config.Namespace = "default"
	}
	return newKubernetesPVCRestoreHelper("https://"+netJoinHostPort(host, port), strings.TrimSpace(string(tokenRaw)), client, config), nil
}

func newKubernetesPVCRestoreHelper(baseURL, token string, client *http.Client, config KubernetesPVCRestoreHelperConfig) *KubernetesPVCRestoreHelper {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Minute
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 2 * time.Second
	}
	if strings.TrimSpace(config.ImagePullPolicy) == "" {
		config.ImagePullPolicy = "IfNotPresent"
	}
	if len(config.PVCAccessModes) == 0 {
		config.PVCAccessModes = []string{"ReadWriteOnce"}
	}
	return &KubernetesPVCRestoreHelper{baseURL: strings.TrimRight(baseURL, "/"), token: strings.TrimSpace(token), client: client, config: config}
}

func (h *KubernetesPVCRestoreHelper) PrepareRepositoryRestoreTarget(ctx context.Context, req ClusterRestorePVCTargetControllerRequest) (ClusterRestorePVCTargetControllerResult, error) {
	result := ClusterRestorePVCTargetControllerResult{NodeEvidence: map[int]map[string]string{}}
	if h == nil {
		return result, fmt.Errorf("Kubernetes PVC restore controller is not configured")
	}
	namespace := firstNonEmptyString(strings.TrimSpace(req.Target.Namespace), strings.TrimSpace(h.config.Namespace), "default")
	statefulSet := strings.TrimSpace(req.Target.StatefulSet)
	now := time.Now().UTC()
	for _, node := range req.Nodes {
		pvcName := strings.TrimSpace(node.TargetPVCName)
		if pvcName == "" {
			blocker := ValidationBlocker{Reason: "target_pvc_name_required", Detail: fmt.Sprintf("restore target ordinal %d has no target_pvc_name", node.Ordinal), FieldPath: fmt.Sprintf("target.nodes[%d].target_pvc_name", node.Ordinal)}
			result.CleanTarget.Blockers = append(result.CleanTarget.Blockers, blocker)
			return result, errors.New(blocker.Detail)
		}
		result.NodeEvidence[node.Ordinal] = map[string]string{"kubernetes_namespace": namespace, "target_pvc_name": pvcName}
		if statefulSet != "" {
			result.NodeEvidence[node.Ordinal]["stateful_set"] = statefulSet
		}
	}
	if statefulSet != "" && h.config.ScaleDownStatefulSet {
		if kubernetesRestoreControllerSelfTargetsStatefulSet(statefulSet) {
			err := fmt.Errorf("Kubernetes restore controller is running in target StatefulSet %q; start restore from a control daemon outside the target StatefulSet before scale-down/PVC replacement", statefulSet)
			result.SourceFencing.Blockers = append(result.SourceFencing.Blockers, ValidationBlocker{Reason: "kubernetes_restore_controller_self_target", Detail: err.Error(), FieldPath: "target.stateful_set"})
			return result, err
		}
		fence, task, err := h.scaleStatefulSetToZero(ctx, namespace, statefulSet, req.Nodes)
		result.CleanupTasks = append(result.CleanupTasks, task)
		if err != nil {
			result.SourceFencing.Blockers = append(result.SourceFencing.Blockers, ValidationBlocker{Reason: "kubernetes_statefulset_scale_down_failed", Detail: err.Error(), FieldPath: "target.stateful_set", Retryable: true})
			return result, err
		}
		result.SourceFencing = fence
		for ordinal := range result.NodeEvidence {
			result.NodeEvidence[ordinal]["source_fencing"] = fence.FenceTokenRef
		}
	}
	allFresh := true
	checked := make([]ClusterRestoreNodeMapping, 0, len(req.Nodes))
	for index, node := range req.Nodes {
		pvcName := strings.TrimSpace(node.TargetPVCName)
		evidence := result.NodeEvidence[node.Ordinal]
		prepared, blockers, err := h.prepareTargetPVC(ctx, namespace, pvcName, req.Target, node)
		for key, value := range prepared {
			evidence[key] = value
		}
		if len(blockers) > 0 {
			result.CleanTarget.Blockers = append(result.CleanTarget.Blockers, blockers...)
		}
		if err != nil {
			if len(blockers) == 0 {
				result.CleanTarget.Blockers = append(result.CleanTarget.Blockers, ValidationBlocker{Reason: "kubernetes_target_pvc_prepare_failed", Detail: err.Error(), FieldPath: fmt.Sprintf("target.nodes[%d].target_pvc_name", index), Retryable: true})
			}
			return result, err
		}
		if evidence["target_pvc_fresh"] != "true" {
			allFresh = false
		}
		checked = append(checked, node)
	}
	result.CleanTarget = ClusterRestoreCleanTargetProof{ProofID: kubernetesRestoreSafeName(req.RestoreOperationID + "-kubernetes-pvc-empty"), CheckedAt: now, TargetEmpty: allFresh, CheckedNodes: checked, Blockers: result.CleanTarget.Blockers, OperatorAttestation: "Kubernetes restore controller created or replaced every target PVC; helper pod verifies the mounted data dir is empty immediately before extraction"}
	if !allFresh && len(result.CleanTarget.Blockers) == 0 {
		result.CleanTarget.Blockers = append(result.CleanTarget.Blockers, ValidationBlocker{Reason: "target_pvc_not_controller_fresh", Detail: "all controller-managed target PVCs must be newly created or explicitly replaced before helper restore", FieldPath: "target.nodes"})
		return result, fmt.Errorf("controller-managed target PVCs are not all fresh")
	}
	for ordinal := range result.NodeEvidence {
		result.NodeEvidence[ordinal]["empty_target_proof_id"] = result.CleanTarget.ProofID
		result.NodeEvidence[ordinal]["empty_target_proof"] = strconv.FormatBool(result.CleanTarget.TargetEmpty)
	}
	return result, nil
}

func (h *KubernetesPVCRestoreHelper) RestoreRepositoryBackupNodeToPVC(ctx context.Context, req ClusterRestorePVCHelperRequest) (result ClusterRestorePVCHelperResult, retErr error) {
	if h == nil {
		return result, fmt.Errorf("Kubernetes PVC restore helper is not configured")
	}
	pvcName := strings.TrimSpace(req.Node.TargetPVCName)
	if pvcName == "" {
		return result, fmt.Errorf("target PVC name is required")
	}
	image := strings.TrimSpace(h.config.HelperImage)
	if image == "" {
		return result, fmt.Errorf("Kubernetes restore helper image is required")
	}
	namespace := firstNonEmptyString(strings.TrimSpace(req.Target.Namespace), strings.TrimSpace(h.config.Namespace), "default")
	podName := kubernetesRestoreSafeName(fmt.Sprintf("mycel-restore-%s-%d", req.RestoreOperationID, req.Node.Ordinal))
	secretName, envSpec, cleanupSecret, err := h.repositoryCredentialEnv(ctx, namespace, podName, req.Repository)
	if err != nil {
		return result, err
	}
	result.TargetPVCName = pvcName
	result.Evidence = map[string]string{"kubernetes_namespace": namespace, "target_pvc_name": pvcName, "helper_pod_name": podName}
	if cleanupSecret {
		defer func() {
			state, lastErr := h.deleteNamespacedResource(context.Background(), namespace, "secrets", secretName)
			result.CleanupTasks = append(result.CleanupTasks, CleanupTask{TaskID: "restore-credentials-secret", ResourceKind: "kubernetes_secret", ResourceURI: "k8s://" + namespace + "/secret/" + secretName, State: state, LastError: lastErr})
		}()
	}
	if h.config.DeleteOnCompletion {
		defer func() {
			state, lastErr := h.deleteNamespacedResource(context.Background(), namespace, "pods", podName)
			result.CleanupTasks = append(result.CleanupTasks, CleanupTask{TaskID: "restore-helper-pod", ResourceKind: "kubernetes_pod", ResourceURI: "k8s://" + namespace + "/pod/" + podName, State: state, LastError: lastErr})
		}()
	}
	args := repositoryRestoreHelperArgs(req, envSpec)
	pod := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      podName,
			"namespace": namespace,
			"labels": map[string]string{
				"app.kubernetes.io/name":       "mycel-restore-helper",
				"app.kubernetes.io/managed-by": "mycel-daemon",
			},
		},
		"spec": map[string]any{
			"restartPolicy": "Never",
			"containers": []map[string]any{{
				"name":            "restore",
				"image":           image,
				"imagePullPolicy": h.config.ImagePullPolicy,
				"command":         []string{"mycel"},
				"args":            args,
				"env":             envSpec.Env,
				"volumeMounts": []map[string]any{{
					"name":      "data",
					"mountPath": defaultKubernetesRestoreDataDir,
				}},
			}},
			"volumes": []map[string]any{{
				"name": "data",
				"persistentVolumeClaim": map[string]string{
					"claimName": pvcName,
				},
			}},
		},
	}
	if serviceAccount := strings.TrimSpace(h.config.ServiceAccountName); serviceAccount != "" {
		pod["spec"].(map[string]any)["serviceAccountName"] = serviceAccount
	}
	if err := h.createNamespacedResource(ctx, namespace, "pods", pod); err != nil {
		return result, fmt.Errorf("create Kubernetes restore helper pod: %w", err)
	}
	phase, logs, err := h.waitForPodCompletion(ctx, namespace, podName)
	result.Evidence["helper_pod_phase"] = phase
	result.Evidence["helper_logs_tail"] = boundedString(logs, 4096)
	if events, eventsErr := h.getPodEvents(ctx, namespace, podName); eventsErr == nil && strings.TrimSpace(events) != "" {
		result.Evidence["helper_events_tail"] = boundedString(events, 4096)
	}
	if err != nil {
		return result, err
	}
	if phase != "Succeeded" {
		return result, fmt.Errorf("Kubernetes restore helper pod finished phase=%s logs=%s", phase, boundedString(logs, 4096))
	}
	result.Evidence["helper_verified"] = "true"
	result.Evidence["empty_target_verified_by"] = "restore-repository-local"
	result.Verified = true
	result.BytesCompleted = parseSignedDecimalStringLocal(req.Artifact.SizeBytes)
	return result, nil
}

func (h *KubernetesPVCRestoreHelper) scaleStatefulSetToZero(ctx context.Context, namespace, statefulSet string, nodes []ClusterRestoreNodeMapping) (ClusterRestoreSourceFencingEvidence, CleanupTask, error) {
	uri := "k8s://" + namespace + "/statefulset/" + statefulSet
	task := CleanupTask{TaskID: "kubernetes-statefulset-scale-down", ResourceKind: "kubernetes_statefulset", ResourceURI: uri, State: "pending"}
	current, exists, err := h.getKubernetesResource(ctx, fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s", url.PathEscape(namespace), url.PathEscape(statefulSet)))
	if err != nil {
		task.State = "failed"
		task.LastError = err.Error()
		return ClusterRestoreSourceFencingEvidence{}, task, err
	}
	if !exists {
		err := fmt.Errorf("target StatefulSet %s/%s was not found", namespace, statefulSet)
		task.State = "failed"
		task.LastError = err.Error()
		return ClusterRestoreSourceFencingEvidence{}, task, err
	}
	before := nestedInt64(current, "spec", "replicas")
	if _, err := h.patchJSON(ctx, fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s/scale", url.PathEscape(namespace), url.PathEscape(statefulSet)), map[string]any{"spec": map[string]any{"replicas": 0}}); err != nil {
		task.State = "failed"
		task.LastError = err.Error()
		return ClusterRestoreSourceFencingEvidence{}, task, err
	}
	deadline := time.Now().Add(h.config.Timeout)
	for {
		latest, latestExists, latestErr := h.getKubernetesResource(ctx, fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s", url.PathEscape(namespace), url.PathEscape(statefulSet)))
		if latestErr != nil {
			task.State = "failed"
			task.LastError = latestErr.Error()
			return ClusterRestoreSourceFencingEvidence{}, task, latestErr
		}
		if !latestExists {
			err := fmt.Errorf("target StatefulSet %s/%s disappeared while waiting for scale-down", namespace, statefulSet)
			task.State = "failed"
			task.LastError = err.Error()
			return ClusterRestoreSourceFencingEvidence{}, task, err
		}
		if nestedInt64(latest, "status", "replicas") == 0 && nestedInt64(latest, "status", "readyReplicas") == 0 {
			gone, goneErr := h.statefulSetPodsGone(ctx, namespace, statefulSet, nodes)
			if goneErr != nil {
				task.State = "failed"
				task.LastError = goneErr.Error()
				return ClusterRestoreSourceFencingEvidence{}, task, goneErr
			}
			if gone {
				task.State = "scaled_down"
				return ClusterRestoreSourceFencingEvidence{SourceClusterFenced: true, FencedAt: time.Now().UTC(), FenceTokenRef: fmt.Sprintf("%s?replicas_before=%d&replicas_after=0", uri, before), FencedEndpoints: []string{uri}}, task, nil
			}
		}
		if time.Now().After(deadline) {
			err := fmt.Errorf("StatefulSet %s/%s did not scale to zero before timeout", namespace, statefulSet)
			task.State = "failed"
			task.LastError = err.Error()
			return ClusterRestoreSourceFencingEvidence{}, task, err
		}
		timer := time.NewTimer(h.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			task.State = "failed"
			task.LastError = ctx.Err().Error()
			return ClusterRestoreSourceFencingEvidence{}, task, ctx.Err()
		case <-timer.C:
		}
	}
}

func (h *KubernetesPVCRestoreHelper) statefulSetPodsGone(ctx context.Context, namespace, statefulSet string, nodes []ClusterRestoreNodeMapping) (bool, error) {
	for _, node := range nodes {
		podName := strings.TrimSpace(node.TargetPodName)
		if podName == "" {
			podName = strings.TrimSpace(node.SourcePodName)
		}
		if podName == "" {
			podName = fmt.Sprintf("%s-%d", statefulSet, node.Ordinal)
		}
		_, exists, err := h.getKubernetesResource(ctx, fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", url.PathEscape(namespace), url.PathEscape(podName)))
		if err != nil {
			return false, err
		}
		if exists {
			return false, nil
		}
	}
	return true, nil
}

func (h *KubernetesPVCRestoreHelper) prepareTargetPVC(ctx context.Context, namespace, pvcName string, target ClusterRestoreTarget, node ClusterRestoreNodeMapping) (map[string]string, []ValidationBlocker, error) {
	evidence := map[string]string{"target_pvc_name": pvcName, "target_pvc_fresh": "false"}
	path := fmt.Sprintf("/api/v1/namespaces/%s/persistentvolumeclaims/%s", url.PathEscape(namespace), url.PathEscape(pvcName))
	existing, exists, err := h.getKubernetesResource(ctx, path)
	if err != nil {
		return evidence, nil, err
	}
	var pvcSpec map[string]any
	if exists {
		evidence["target_pvc_uid_before"] = nestedString(existing, "metadata", "uid")
		if !h.config.ReplaceExistingPVCs {
			blocker := ValidationBlocker{Reason: "target_pvc_exists_replacement_not_enabled", Detail: fmt.Sprintf("target PVC %s/%s already exists; set MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_REPLACEMENT_ENABLED=true to let the daemon replace it or provide an external clean-target proof", namespace, pvcName), FieldPath: "target.nodes.target_pvc_name"}
			return evidence, []ValidationBlocker{blocker}, errors.New(blocker.Detail)
		}
		if !h.config.ProvisionPVCs {
			blocker := ValidationBlocker{Reason: "target_pvc_replacement_requires_provisioning", Detail: fmt.Sprintf("target PVC %s/%s replacement requires PVC provisioning to recreate the claim after deletion", namespace, pvcName), FieldPath: "target.nodes.target_pvc_name"}
			return evidence, []ValidationBlocker{blocker}, errors.New(blocker.Detail)
		}
		pvcSpec = sanitizePVCSpec(existing)
		state, lastErr := h.deleteNamespacedResource(ctx, namespace, "persistentvolumeclaims", pvcName)
		evidence["target_pvc_delete_state"] = state
		if lastErr != "" {
			return evidence, nil, errors.New(lastErr)
		}
		if err := h.waitForPVCDeleted(ctx, namespace, pvcName); err != nil {
			return evidence, nil, err
		}
		evidence["target_pvc_action"] = "replaced"
	} else {
		evidence["target_pvc_action"] = "created"
	}
	if !h.config.ProvisionPVCs {
		blocker := ValidationBlocker{Reason: "target_pvc_provisioning_disabled", Detail: fmt.Sprintf("target PVC %s/%s does not exist and controller PVC provisioning is disabled", namespace, pvcName), FieldPath: "target.nodes.target_pvc_name"}
		return evidence, []ValidationBlocker{blocker}, errors.New(blocker.Detail)
	}
	if pvcSpec == nil {
		var blockers []ValidationBlocker
		pvcSpec, blockers = h.pvcSpecFromConfig(target, node)
		if len(blockers) > 0 {
			return evidence, blockers, errors.New(blockers[0].Detail)
		}
	}
	pvc := map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"name": pvcName, "namespace": namespace, "labels": restorePVCLabels(target.Labels)}, "spec": pvcSpec}
	if err := h.createNamespacedResource(ctx, namespace, "persistentvolumeclaims", pvc); err != nil {
		return evidence, nil, err
	}
	created, err := h.waitForPVCExists(ctx, namespace, pvcName)
	if err != nil {
		return evidence, nil, err
	}
	evidence["target_pvc_uid_after"] = nestedString(created, "metadata", "uid")
	evidence["target_pvc_phase"] = nestedString(created, "status", "phase")
	evidence["target_pvc_fresh"] = "true"
	return evidence, nil, nil
}

func (h *KubernetesPVCRestoreHelper) pvcSpecFromConfig(target ClusterRestoreTarget, node ClusterRestoreNodeMapping) (map[string]any, []ValidationBlocker) {
	storage := firstNonEmptyString(strings.TrimSpace(target.Annotations["myceldb.io/restore-pvc-storage-request"]), strings.TrimSpace(h.config.PVCStorageRequest))
	if storage == "" {
		blocker := ValidationBlocker{Reason: "target_pvc_provisioning_requires_storage_request", Detail: "controller-managed Kubernetes restore cannot create a missing target PVC without MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_STORAGE_REQUEST or target annotation myceldb.io/restore-pvc-storage-request", FieldPath: "target.annotations[myceldb.io/restore-pvc-storage-request]"}
		return nil, []ValidationBlocker{blocker}
	}
	accessModes := append([]string(nil), h.config.PVCAccessModes...)
	if modes := strings.TrimSpace(target.Annotations["myceldb.io/restore-pvc-access-modes"]); modes != "" {
		accessModes = splitCSVLocal(modes)
	}
	if len(accessModes) == 0 {
		accessModes = []string{"ReadWriteOnce"}
	}
	spec := map[string]any{"accessModes": accessModes, "resources": map[string]any{"requests": map[string]any{"storage": storage}}}
	if className := firstNonEmptyString(strings.TrimSpace(target.Annotations["myceldb.io/restore-pvc-storage-class"]), strings.TrimSpace(h.config.PVCStorageClassName)); className != "" {
		spec["storageClassName"] = className
	}
	return spec, nil
}

func restorePVCLabels(labels map[string]string) map[string]string {
	out := map[string]string{"app.kubernetes.io/name": "mycel", "app.kubernetes.io/managed-by": "mycel-daemon", "app.kubernetes.io/component": "restore-target-pvc"}
	for key, value := range labels {
		if strings.TrimSpace(key) != "" && strings.TrimSpace(value) != "" {
			out[key] = value
		}
	}
	return out
}

func sanitizePVCSpec(pvc map[string]any) map[string]any {
	spec, _ := pvc["spec"].(map[string]any)
	out := map[string]any{}
	for _, key := range []string{"accessModes", "resources", "storageClassName", "volumeMode"} {
		if value, ok := spec[key]; ok {
			if nested, ok := value.(map[string]any); ok {
				out[key] = cloneAnyMap(nested)
			} else {
				out[key] = value
			}
		}
	}
	return out
}

func (h *KubernetesPVCRestoreHelper) waitForPVCExists(ctx context.Context, namespace, pvcName string) (map[string]any, error) {
	deadline := time.Now().Add(h.config.Timeout)
	path := fmt.Sprintf("/api/v1/namespaces/%s/persistentvolumeclaims/%s", url.PathEscape(namespace), url.PathEscape(pvcName))
	for {
		pvc, exists, err := h.getKubernetesResource(ctx, path)
		if err != nil {
			return nil, err
		}
		if exists {
			return pvc, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("PVC %s/%s was not created before timeout", namespace, pvcName)
		}
		timer := time.NewTimer(h.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (h *KubernetesPVCRestoreHelper) waitForPVCDeleted(ctx context.Context, namespace, pvcName string) error {
	deadline := time.Now().Add(h.config.Timeout)
	path := fmt.Sprintf("/api/v1/namespaces/%s/persistentvolumeclaims/%s", url.PathEscape(namespace), url.PathEscape(pvcName))
	for {
		_, exists, err := h.getKubernetesResource(ctx, path)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("PVC %s/%s was not deleted before timeout", namespace, pvcName)
		}
		timer := time.NewTimer(h.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (h *KubernetesPVCRestoreHelper) getKubernetesResource(ctx context.Context, path string) (map[string]any, bool, error) {
	body, statusCode, _, err := h.doJSONStatus(ctx, http.MethodGet, path, "", nil)
	if statusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

func (h *KubernetesPVCRestoreHelper) patchJSON(ctx context.Context, path string, body any) (map[string]any, error) {
	out, _, _, err := h.doJSONStatus(ctx, http.MethodPatch, path, "application/merge-patch+json", body)
	return out, err
}

type kubernetesRestoreCredentialEnv struct {
	AccessKeyEnv string
	SecretKeyEnv string
	SessionEnv   string
	Env          []map[string]any
}

func (h *KubernetesPVCRestoreHelper) repositoryCredentialEnv(ctx context.Context, namespace, podName string, repo BackupRepository) (string, kubernetesRestoreCredentialEnv, bool, error) {
	ref := normalizeSecretReference(repo.CredentialRef)
	out := kubernetesRestoreCredentialEnv{AccessKeyEnv: "AWS_ACCESS_KEY_ID", SecretKeyEnv: "AWS_SECRET_ACCESS_KEY", SessionEnv: "AWS_SESSION_TOKEN"}
	switch ref.Scheme {
	case SecretSchemeEnv:
		accessEnv := strings.TrimSpace(ref.Keys["access_key_id"])
		secretEnv := strings.TrimSpace(ref.Keys["secret_access_key"])
		sessionEnv := strings.TrimSpace(ref.Keys["session_token"])
		if accessEnv == "" || secretEnv == "" {
			return "", out, false, fmt.Errorf("env credential reference requires access_key_id and secret_access_key names")
		}
		accessValue := os.Getenv(accessEnv)
		secretValue := os.Getenv(secretEnv)
		if strings.TrimSpace(accessValue) == "" || strings.TrimSpace(secretValue) == "" {
			return "", out, false, fmt.Errorf("env credential reference values are not available to create restore helper secret")
		}
		secretName := kubernetesRestoreSafeName("mycel-restore-creds-" + podName)
		stringData := map[string]string{"access_key_id": accessValue, "secret_access_key": secretValue}
		if sessionEnv != "" && strings.TrimSpace(os.Getenv(sessionEnv)) != "" {
			stringData["session_token"] = os.Getenv(sessionEnv)
		}
		secret := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": secretName, "namespace": namespace}, "type": "Opaque", "stringData": stringData}
		if err := h.createNamespacedResource(ctx, namespace, "secrets", secret); err != nil {
			return "", out, false, fmt.Errorf("create restore helper credential secret: %w", err)
		}
		out.AccessKeyEnv, out.SecretKeyEnv, out.SessionEnv = accessEnv, secretEnv, sessionEnv
		out.Env = []map[string]any{secretEnvVar(accessEnv, secretName, "access_key_id"), secretEnvVar(secretEnv, secretName, "secret_access_key")}
		if sessionEnv != "" && stringData["session_token"] != "" {
			out.Env = append(out.Env, secretEnvVar(sessionEnv, secretName, "session_token"))
		}
		return secretName, out, true, nil
	case SecretSchemeK8sSecret:
		secretName := strings.TrimSpace(ref.Name)
		if secretName == "" {
			return "", out, false, fmt.Errorf("k8s-secret credential reference requires name")
		}
		if ref.Namespace != "" && ref.Namespace != namespace {
			return "", out, false, fmt.Errorf("k8s-secret credential reference namespace %q does not match restore helper namespace %q", ref.Namespace, namespace)
		}
		accessKey := firstNonEmptyString(ref.Keys["access_key_id"], "AWS_ACCESS_KEY_ID", "access_key_id")
		secretKey := firstNonEmptyString(ref.Keys["secret_access_key"], "AWS_SECRET_ACCESS_KEY", "secret_access_key")
		sessionKey := strings.TrimSpace(ref.Keys["session_token"])
		out.Env = []map[string]any{secretEnvVar(out.AccessKeyEnv, secretName, accessKey), secretEnvVar(out.SecretKeyEnv, secretName, secretKey)}
		if sessionKey != "" {
			out.Env = append(out.Env, secretEnvVar(out.SessionEnv, secretName, sessionKey))
		}
		return secretName, out, false, nil
	case SecretSchemeAWSIRSA:
		out.AccessKeyEnv, out.SecretKeyEnv, out.SessionEnv = "", "", ""
		return "", out, false, nil
	default:
		return "", out, false, fmt.Errorf("credential reference scheme %q is not supported by Kubernetes PVC restore helper", ref.Scheme)
	}
}

func repositoryRestoreHelperArgs(req ClusterRestorePVCHelperRequest, envSpec kubernetesRestoreCredentialEnv) []string {
	args := []string{"--output", "json", "admin", "backup", "cluster", "restore-repository-local", "--bucket", req.Repository.S3.Bucket, "--backup-set-id", req.BackupSetID, "--backup-set-prefix", req.BackupSetPrefix, "--repository-prefix", req.Repository.S3.Prefix, "--endpoint-uri", req.Repository.S3.EndpointURI, "--region", firstNonEmptyString(req.Repository.S3.Region, "us-east-1"), "--ordinal", strconv.Itoa(req.Node.Ordinal), "--data-dir", defaultKubernetesRestoreDataDir, "--target-pvc-name", req.Node.TargetPVCName}
	if req.Repository.S3.ForcePathStyle {
		args = append(args, "--force-path-style")
	}
	if envSpec.AccessKeyEnv != "" {
		args = append(args, "--access-key-env", envSpec.AccessKeyEnv)
	}
	if envSpec.SecretKeyEnv != "" {
		args = append(args, "--secret-key-env", envSpec.SecretKeyEnv)
	}
	if envSpec.SessionEnv != "" {
		args = append(args, "--session-token-env", envSpec.SessionEnv)
	}
	return args
}

func secretEnvVar(name, secretName, key string) map[string]any {
	return map[string]any{"name": name, "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": secretName, "key": key}}}
}

func (h *KubernetesPVCRestoreHelper) createNamespacedResource(ctx context.Context, namespace, resource string, body map[string]any) error {
	_, err := h.doJSON(ctx, http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%s/%s", url.PathEscape(namespace), resource), body)
	return err
}

func (h *KubernetesPVCRestoreHelper) deleteNamespacedResource(ctx context.Context, namespace, resource, name string) (string, string) {
	if strings.TrimSpace(name) == "" {
		return "skipped", ""
	}
	_, err := h.doJSON(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%s/%s/%s", url.PathEscape(namespace), resource, url.PathEscape(name)), map[string]any{"kind": "DeleteOptions", "apiVersion": "v1"})
	if err != nil {
		return "failed", err.Error()
	}
	return "deleted", ""
}

func (h *KubernetesPVCRestoreHelper) waitForPodCompletion(ctx context.Context, namespace, podName string) (string, string, error) {
	deadline := time.Now().Add(h.config.Timeout)
	for {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		raw, err := h.doJSON(ctx, http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", url.PathEscape(namespace), url.PathEscape(podName)), nil)
		if err != nil {
			return "", "", err
		}
		phase := nestedString(raw, "status", "phase")
		if phase == "Succeeded" || phase == "Failed" {
			logs, _ := h.getPodLogs(ctx, namespace, podName)
			return phase, logs, nil
		}
		if time.Now().After(deadline) {
			logs, _ := h.getPodLogs(ctx, namespace, podName)
			return phase, logs, fmt.Errorf("Kubernetes restore helper pod %s did not complete before timeout; phase=%s logs=%s", podName, phase, boundedString(logs, 4096))
		}
		timer := time.NewTimer(h.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", "", ctx.Err()
		case <-timer.C:
		}
	}
}

func (h *KubernetesPVCRestoreHelper) getPodLogs(ctx context.Context, namespace, podName string) (string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?container=restore&tailLines=200", url.PathEscape(namespace), url.PathEscape(podName))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+path, nil)
	if err != nil {
		return "", err
	}
	h.authorize(request)
	response, err := h.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if response.StatusCode >= 300 {
		return string(raw), fmt.Errorf("Kubernetes logs request failed: %s", response.Status)
	}
	return string(raw), nil
}

func (h *KubernetesPVCRestoreHelper) getPodEvents(ctx context.Context, namespace, podName string) (string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/events?fieldSelector=involvedObject.name=%s", url.PathEscape(namespace), url.QueryEscape(podName))
	_, statusCode, raw, err := h.doJSONStatus(ctx, http.MethodGet, path, "", nil)
	if err != nil && statusCode != http.StatusNotFound {
		return "", err
	}
	return raw, nil
}

func (h *KubernetesPVCRestoreHelper) doJSON(ctx context.Context, method, path string, body any) (map[string]any, error) {
	out, _, _, err := h.doJSONStatus(ctx, method, path, "application/json", body)
	return out, err
}

func (h *KubernetesPVCRestoreHelper) doJSONStatus(ctx context.Context, method, path, contentType string, body any) (map[string]any, int, string, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, 0, "", err
		}
		reader = bytes.NewReader(raw)
	}
	request, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, reader)
	if err != nil {
		return nil, 0, "", err
	}
	h.authorize(request)
	if body != nil {
		if strings.TrimSpace(contentType) == "" {
			contentType = "application/json"
		}
		request.Header.Set("Content-Type", contentType)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return nil, 0, "", err
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 512*1024))
	if response.StatusCode >= 300 {
		return nil, response.StatusCode, string(raw), fmt.Errorf("Kubernetes API %s %s failed: %s: %s", method, path, response.Status, boundedString(string(raw), 4096))
	}
	if len(raw) == 0 {
		return map[string]any{}, response.StatusCode, "", nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, response.StatusCode, string(raw), fmt.Errorf("decode Kubernetes API response: %w", err)
	}
	return out, response.StatusCode, string(raw), nil
}

func (h *KubernetesPVCRestoreHelper) authorize(request *http.Request) {
	if h.token != "" {
		request.Header.Set("Authorization", "Bearer "+h.token)
	}
}

func nestedString(values map[string]any, path ...string) string {
	var current any = values
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = object[key]
	}
	value, _ := current.(string)
	return value
}

func nestedInt64(values map[string]any, path ...string) int64 {
	var current any = values
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return 0
		}
		current = object[key]
	}
	switch value := current.(type) {
	case float64:
		return int64(value)
	case int64:
		return value
	case int:
		return int64(value)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed
	default:
		return 0
	}
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		if nested, ok := value.(map[string]any); ok {
			out[key] = cloneAnyMap(nested)
			continue
		}
		out[key] = value
	}
	return out
}

func splitCSVLocal(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func kubernetesRestoreControllerSelfTargetsStatefulSet(statefulSet string) bool {
	statefulSet = strings.TrimSpace(statefulSet)
	hostname := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if statefulSet == "" || hostname == "" || !strings.HasPrefix(hostname, statefulSet+"-") {
		return false
	}
	suffix := strings.TrimPrefix(hostname, statefulSet+"-")
	if suffix == "" {
		return false
	}
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func kubernetesRestoreSafeName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	if out == "" {
		return "mycel-restore"
	}
	return out
}

func netJoinHostPort(host, port string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return host + ":" + port
}

func boundedString(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "...(truncated)"
}
