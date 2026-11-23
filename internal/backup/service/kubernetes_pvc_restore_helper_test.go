package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
)

func TestKubernetesPVCRestoreHelperCreatesPodWithCredentialSecret(t *testing.T) {
	t.Setenv("MINIO_ACCESS_KEY", "minio-access")
	t.Setenv("MINIO_SECRET_KEY", "minio-secret")
	var sawSecret bool
	var sawPod bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("Authorization header = %q, want bearer token", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/restore/secrets":
			sawSecret = true
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode secret: %v", err)
			}
			stringData, _ := body["stringData"].(map[string]any)
			if got := stringData["access_key_id"]; got != "minio-access" {
				t.Fatalf("secret access_key_id=%v", got)
			}
			if got := stringData["secret_access_key"]; got != "minio-secret" {
				t.Fatalf("secret secret_access_key=%v", got)
			}
			_, _ = io.WriteString(w, `{"metadata":{"name":"created-secret"}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/restore/pods":
			sawPod = true
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read pod: %v", err)
			}
			if strings.Contains(string(raw), "minio-secret") || strings.Contains(string(raw), "minio-access") {
				t.Fatalf("pod spec leaked raw credentials: %s", string(raw))
			}
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Fatalf("decode pod: %v", err)
			}
			spec := body["spec"].(map[string]any)
			containers := spec["containers"].([]any)
			container := containers[0].(map[string]any)
			args := container["args"].([]any)
			if !containsAnyString(args, "--target-pvc-name") || !containsAnyString(args, "data-myceld-0") {
				t.Fatalf("pod args missing target pvc: %#v", args)
			}
			_, _ = io.WriteString(w, `{"metadata":{"name":"mycel-restore-op-0"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/restore/pods/mycel-restore-op-0":
			_, _ = io.WriteString(w, `{"status":{"phase":"Succeeded"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/restore/pods/mycel-restore-op-0/log":
			_, _ = io.WriteString(w, "restore complete")
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/restore/events":
			_, _ = io.WriteString(w, `{"items":[{"message":"pod scheduled"}]}`)
		case r.Method == http.MethodDelete && (r.URL.Path == "/api/v1/namespaces/restore/pods/mycel-restore-op-0" || strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/restore/secrets/")):
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Fatalf("unexpected Kubernetes request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	helper := newKubernetesPVCRestoreHelper(server.URL, "test-token", server.Client(), KubernetesPVCRestoreHelperConfig{Namespace: "restore", HelperImage: "local/mycel:test", Timeout: time.Second, PollInterval: time.Millisecond, DeleteOnCompletion: true})
	result, err := helper.RestoreRepositoryBackupNodeToPVC(context.Background(), ClusterRestorePVCHelperRequest{
		RestoreOperationID: "op",
		BackupSetID:        "backup-1",
		BackupSetPrefix:    "clusters/test/backup-1",
		Repository:         BackupRepository{S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Prefix: "clusters/test", EndpointURI: "http://minio:9000", Region: "us-east-1", ForcePathStyle: true}, CredentialRef: SecretReference{Scheme: SecretSchemeEnv, Keys: map[string]string{"access_key_id": "MINIO_ACCESS_KEY", "secret_access_key": "MINIO_SECRET_KEY"}}},
		Target:             ClusterRestoreTarget{Namespace: "restore"},
		Node:               ClusterRestoreNodeMapping{Ordinal: 0, TargetPVCName: "data-myceld-0"},
		Artifact:           clusterbackup.BackupSetNodeArtifact{SizeBytes: "1234"},
	})
	if err != nil {
		t.Fatalf("RestoreRepositoryBackupNodeToPVC returned error: %v", err)
	}
	if !sawSecret || !sawPod {
		t.Fatalf("sawSecret=%v sawPod=%v", sawSecret, sawPod)
	}
	if !result.Verified || result.BytesCompleted != 1234 || result.Evidence["helper_pod_phase"] != "Succeeded" || result.Evidence["helper_logs_tail"] != "restore complete" {
		t.Fatalf("result=%#v", result)
	}
	if len(result.CleanupTasks) != 2 {
		t.Fatalf("cleanup tasks=%#v, want pod and secret cleanup", result.CleanupTasks)
	}
}

func TestKubernetesPVCRestoreHelperPreparesControllerManagedPVCs(t *testing.T) {
	var sawScalePatch bool
	var sawPVCCreate bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/restore/statefulsets/myceld":
			_, _ = io.WriteString(w, `{"spec":{"replicas":3},"status":{"replicas":0,"readyReplicas":0}}`)
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/apps/v1/namespaces/restore/statefulsets/myceld/scale":
			sawScalePatch = true
			if got := r.Header.Get("Content-Type"); !strings.Contains(got, "application/merge-patch+json") {
				t.Fatalf("scale patch content-type=%q", got)
			}
			_, _ = io.WriteString(w, `{"status":{"replicas":0}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/restore/pods/myceld-0":
			http.NotFound(w, r)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/restore/persistentvolumeclaims/data-myceld-0":
			if sawPVCCreate {
				_, _ = io.WriteString(w, `{"metadata":{"uid":"pvc-created"},"status":{"phase":"Pending"}}`)
				return
			}
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/restore/persistentvolumeclaims":
			sawPVCCreate = true
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode pvc: %v", err)
			}
			spec := body["spec"].(map[string]any)
			if spec["storageClassName"] != "gp3" {
				t.Fatalf("storageClassName=%v", spec["storageClassName"])
			}
			_, _ = io.WriteString(w, `{"metadata":{"uid":"pvc-created"}}`)
		default:
			t.Fatalf("unexpected Kubernetes request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	helper := newKubernetesPVCRestoreHelper(server.URL, "", server.Client(), KubernetesPVCRestoreHelperConfig{Namespace: "restore", Timeout: time.Second, PollInterval: time.Millisecond, ScaleDownStatefulSet: true, ProvisionPVCs: true, PVCStorageClassName: "gp3", PVCStorageRequest: "10Gi", PVCAccessModes: []string{"ReadWriteOnce"}})
	result, err := helper.PrepareRepositoryRestoreTarget(context.Background(), ClusterRestorePVCTargetControllerRequest{RestoreOperationID: "restore-op", Target: ClusterRestoreTarget{Namespace: "restore", StatefulSet: "myceld"}, Nodes: []ClusterRestoreNodeMapping{{Ordinal: 0, TargetPVCName: "data-myceld-0"}}})
	if err != nil {
		t.Fatalf("PrepareRepositoryRestoreTarget returned error: %v", err)
	}
	if !sawScalePatch || !sawPVCCreate || !result.SourceFencing.SourceClusterFenced || !result.CleanTarget.TargetEmpty {
		t.Fatalf("unexpected controller result: sawScalePatch=%v sawPVCCreate=%v result=%#v", sawScalePatch, sawPVCCreate, result)
	}
	if result.NodeEvidence[0]["target_pvc_fresh"] != "true" || result.NodeEvidence[0]["source_fencing"] == "" {
		t.Fatalf("missing node evidence: %#v", result.NodeEvidence[0])
	}
}

func TestKubernetesPVCRestoreHelperRefusesExistingPVCWithoutReplacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/restore/persistentvolumeclaims/data-myceld-0":
			_, _ = io.WriteString(w, `{"metadata":{"uid":"existing-pvc"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"10Gi"}}}}`)
		default:
			t.Fatalf("unexpected Kubernetes request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	helper := newKubernetesPVCRestoreHelper(server.URL, "", server.Client(), KubernetesPVCRestoreHelperConfig{Namespace: "restore", Timeout: time.Second, PollInterval: time.Millisecond, ScaleDownStatefulSet: false, ProvisionPVCs: true, ReplaceExistingPVCs: false, PVCStorageRequest: "10Gi"})
	result, err := helper.PrepareRepositoryRestoreTarget(context.Background(), ClusterRestorePVCTargetControllerRequest{RestoreOperationID: "restore-op", Target: ClusterRestoreTarget{Namespace: "restore"}, Nodes: []ClusterRestoreNodeMapping{{Ordinal: 0, TargetPVCName: "data-myceld-0"}}})
	if err == nil {
		t.Fatal("expected existing PVC without replacement to fail closed")
	}
	if len(result.CleanTarget.Blockers) == 0 || result.CleanTarget.Blockers[0].Reason != "target_pvc_exists_replacement_not_enabled" {
		t.Fatalf("unexpected blockers: %#v", result.CleanTarget.Blockers)
	}
}

func TestSanitizePVCSpecOmitsBoundVolumeAndDataSources(t *testing.T) {
	spec := sanitizePVCSpec(map[string]any{"spec": map[string]any{"accessModes": []any{"ReadWriteOnce"}, "storageClassName": "gp3", "volumeMode": "Filesystem", "resources": map[string]any{"requests": map[string]any{"storage": "10Gi"}}, "volumeName": "old-pv", "selector": map[string]any{"matchLabels": map[string]any{"unsafe": "true"}}, "dataSource": map[string]any{"name": "snapshot"}, "dataSourceRef": map[string]any{"name": "snapshot"}}})
	for _, forbidden := range []string{"volumeName", "selector", "dataSource", "dataSourceRef"} {
		if _, ok := spec[forbidden]; ok {
			t.Fatalf("sanitizePVCSpec retained %s: %#v", forbidden, spec)
		}
	}
	if spec["storageClassName"] != "gp3" || spec["volumeMode"] != "Filesystem" || spec["resources"] == nil {
		t.Fatalf("sanitizePVCSpec dropped safe fields: %#v", spec)
	}
}

func TestKubernetesPVCRestoreHelperRefusesSelfTargetStatefulSetScaleDown(t *testing.T) {
	t.Setenv("HOSTNAME", "myceld-1")
	helper := newKubernetesPVCRestoreHelper("http://127.0.0.1", "", http.DefaultClient, KubernetesPVCRestoreHelperConfig{Namespace: "restore", Timeout: time.Second, PollInterval: time.Millisecond, ScaleDownStatefulSet: true, ProvisionPVCs: true, PVCStorageRequest: "10Gi"})
	result, err := helper.PrepareRepositoryRestoreTarget(context.Background(), ClusterRestorePVCTargetControllerRequest{RestoreOperationID: "restore-op", Target: ClusterRestoreTarget{Namespace: "restore", StatefulSet: "myceld"}, Nodes: []ClusterRestoreNodeMapping{{Ordinal: 0, TargetPVCName: "data-myceld-0"}}})
	if err == nil {
		t.Fatal("expected self-target StatefulSet restore controller to fail closed")
	}
	if len(result.SourceFencing.Blockers) == 0 || result.SourceFencing.Blockers[0].Reason != "kubernetes_restore_controller_self_target" {
		t.Fatalf("unexpected source fencing blockers: %#v", result.SourceFencing.Blockers)
	}
}

func TestRepositoryRestoreHelperArgsOmitsStaticCredentialFlagsForIRSA(t *testing.T) {
	args := repositoryRestoreHelperArgs(ClusterRestorePVCHelperRequest{
		BackupSetID:     "backup-1",
		BackupSetPrefix: "clusters/test/backup-1",
		Repository:      BackupRepository{S3: S3CompatibleRepositoryConfig{Bucket: "mycel-backups", Region: "us-east-1"}},
		Node:            ClusterRestoreNodeMapping{Ordinal: 2, TargetPVCName: "data-myceld-2"},
	}, kubernetesRestoreCredentialEnv{})
	if containsString(args, "--access-key-env") || containsString(args, "--secret-key-env") || containsString(args, "--session-token-env") {
		t.Fatalf("IRSA/default-provider args should not force static credential flags: %#v", args)
	}
}

func containsAnyString(values []any, want string) bool {
	for _, value := range values {
		if text, ok := value.(string); ok && text == want {
			return true
		}
	}
	return false
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
