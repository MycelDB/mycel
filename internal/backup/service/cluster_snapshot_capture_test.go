package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestKubernetesSnapshotCaptureProviderPublishesCredentialFreeEvidence(t *testing.T) {
	var createCount atomic.Int32
	var deleteCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("Authorization header=%q, want bearer token", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case r.Method == http.MethodGet && r.URL.Path == "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses/ebs-csi":
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "ebs-csi"}})
		case r.Method == http.MethodPost && r.URL.Path == "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots":
			createCount.Add(1)
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			spec := body["spec"].(map[string]any)
			source := spec["source"].(map[string]any)
			if source["persistentVolumeClaimName"] != "myceld-data-myceld-0" || spec["volumeSnapshotClassName"] != "ebs-csi" {
				t.Fatalf("snapshot spec=%v, want source PVC and class", spec)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": body["metadata"]})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots/mycel-snap-"):
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"readyToUse": true, "boundVolumeSnapshotContentName": "snapcontent-1", "restoreSize": "10Gi", "creationTime": "2026-11-22T10:00:00Z"}})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots/mycel-snap-"):
			deleteCount.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "Success"})
		default:
			t.Fatalf("unexpected Kubernetes request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := newKubernetesSnapshotCaptureProvider(server.URL, "test-token", server.Client(), KubernetesSnapshotCaptureProviderConfig{Namespace: "backup", SnapshotClassName: "ebs-csi", SourcePVCTemplate: "myceld-data-{pod}", DeleteOnCompletion: true, Timeout: time.Second, PollInterval: time.Millisecond})
	result, err := provider.CaptureClusterSnapshot(context.Background(), ClusterSnapshotCaptureRequest{BackupSetID: "backup-set-20261122T100000Z-cluster", ClusterID: "cluster", Namespace: "backup", StatefulSet: "myceld", CaptureProvider: clusterCaptureProviderCSI, Nodes: []ClusterBackupNode{{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1}}})
	if err != nil {
		t.Fatalf("CaptureClusterSnapshot() error = %v", err)
	}
	if result.Provider != clusterCaptureProviderCSI || result.ProviderVersion != "snapshot.storage.k8s.io/v1" || len(result.NodeSources) != 1 || !strings.Contains(string(result.Evidence), "snapcontent-1") {
		t.Fatalf("snapshot capture result=%#v evidence=%s, want CSI evidence", result, string(result.Evidence))
	}
	cleanup := provider.CleanupClusterSnapshot(context.Background(), result)
	if !cleanup.Attempted || !cleanup.Completed || len(cleanup.Items) != 1 || cleanup.Items[0].State != "deleted" {
		t.Fatalf("cleanup=%#v, want completed delete", cleanup)
	}
	if createCount.Load() != 1 || deleteCount.Load() != 1 {
		t.Fatalf("createCount=%d deleteCount=%d, want 1/1", createCount.Load(), deleteCount.Load())
	}
}

func TestKubernetesSnapshotCaptureProviderFailsClosedAndCleansPartialSnapshot(t *testing.T) {
	var deleteCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots":
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "created"}})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots/mycel-snap-"):
			http.Error(w, "snapshot controller unavailable", http.StatusInternalServerError)
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apis/snapshot.storage.k8s.io/v1/namespaces/backup/volumesnapshots/mycel-snap-"):
			deleteCount.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "Success"})
		default:
			t.Fatalf("unexpected Kubernetes request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := newKubernetesSnapshotCaptureProvider(server.URL, "", server.Client(), KubernetesSnapshotCaptureProviderConfig{Namespace: "backup", DeleteOnCompletion: true, Timeout: time.Second, PollInterval: time.Millisecond})
	_, err := provider.CaptureClusterSnapshot(context.Background(), ClusterSnapshotCaptureRequest{BackupSetID: "backup-set-fail", ClusterID: "cluster", Namespace: "backup", CaptureProvider: clusterCaptureProviderCSI, Nodes: []ClusterBackupNode{{PodName: "myceld-0", NodeID: "node-1", Ordinal: 0, RaftNodeID: 1}}})
	if err == nil || !strings.Contains(err.Error(), "snapshot controller unavailable") {
		t.Fatalf("CaptureClusterSnapshot() error=%v, want controller failure", err)
	}
	if deleteCount.Load() != 1 {
		t.Fatalf("partial snapshot cleanup deleteCount=%d, want 1", deleteCount.Load())
	}
}
