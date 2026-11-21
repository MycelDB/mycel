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
	if !result.Verified || result.BytesCompleted != 1234 {
		t.Fatalf("result=%#v", result)
	}
	if len(result.CleanupTasks) != 2 {
		t.Fatalf("cleanup tasks=%#v, want pod and secret cleanup", result.CleanupTasks)
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
