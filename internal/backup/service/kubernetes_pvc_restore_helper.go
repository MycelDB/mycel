package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
	Namespace          string
	HelperImage        string
	ServiceAccountName string
	ImagePullPolicy    string
	Timeout            time.Duration
	PollInterval       time.Duration
	DeleteOnCompletion bool
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
	return &KubernetesPVCRestoreHelper{baseURL: strings.TrimRight(baseURL, "/"), token: strings.TrimSpace(token), client: client, config: config}
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
	if err != nil {
		return result, err
	}
	if phase != "Succeeded" {
		return result, fmt.Errorf("Kubernetes restore helper pod finished phase=%s logs=%s", phase, boundedString(logs, 4096))
	}
	result.Verified = true
	result.BytesCompleted = parseSignedDecimalStringLocal(req.Artifact.SizeBytes)
	return result, nil
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

func (h *KubernetesPVCRestoreHelper) doJSON(ctx context.Context, method, path string, body any) (map[string]any, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	request, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	h.authorize(request)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 512*1024))
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("Kubernetes API %s %s failed: %s: %s", method, path, response.Status, boundedString(string(raw), 4096))
	}
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode Kubernetes API response: %w", err)
	}
	return out, nil
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
