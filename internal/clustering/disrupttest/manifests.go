package disrupttest

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

type ManifestConfig struct {
	Namespace       string
	Image           string
	AdminUsername   string
	AdminPassword   string
	EncryptionKey   string
	BackendToken    string
	NodeCount       int
	PartitionCount  int
	StatefulSet     string
	Service         string
	HeadlessService string
	SelectorApp     string
	ClusterName     string
	NodeAddrs       string
}

func ManifestConfigFromCluster(cfg ClusterConfig, statefulSet, service string, selector string) ManifestConfig {
	if statefulSet == "" {
		statefulSet = DefaultStatefulSet
	}
	if service == "" {
		service = DefaultService
	}
	headless := statefulSet + "-headless"
	if cfg.NodeCount == 0 {
		cfg.NodeCount = DefaultNodeCount
	}
	addrs := make([]string, 0, cfg.NodeCount)
	for i := 0; i < cfg.NodeCount; i++ {
		addrs = append(addrs, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:9091", statefulSet, i, headless, cfg.Namespace))
	}
	selectorApp, err := appSelectorValue(selector)
	if err != nil {
		selectorApp = statefulSet
	}
	return ManifestConfig{Namespace: cfg.Namespace, Image: cfg.Image, AdminUsername: cfg.AdminUsername, AdminPassword: cfg.AdminPassword, EncryptionKey: cfg.EncryptionKey, BackendToken: cfg.BackendToken, NodeCount: cfg.NodeCount, PartitionCount: cfg.PartitionCount, StatefulSet: statefulSet, Service: service, HeadlessService: headless, SelectorApp: selectorApp, ClusterName: cfg.Name, NodeAddrs: strings.Join(addrs, ",")}
}

func RenderManifests(cfg ManifestConfig) (string, error) {
	if cfg.NodeCount <= 0 {
		return "", fmt.Errorf("node count must be positive")
	}
	if cfg.PartitionCount <= 0 {
		return "", fmt.Errorf("partition count must be positive")
	}
	tmpl, err := template.New("manifests").Parse(manifestTemplate)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, cfg); err != nil {
		return "", err
	}
	return buf.String(), nil
}

const manifestTemplate = `apiVersion: v1
kind: Namespace
metadata:
  name: {{ .Namespace }}
---
apiVersion: v1
kind: Secret
metadata:
  name: myceld-secret
  namespace: {{ .Namespace }}
type: Opaque
stringData:
  bootstrap-admin-username: {{ .AdminUsername | printf "%q" }}
  bootstrap-admin-password: {{ .AdminPassword | printf "%q" }}
  user-store-encryption-key-b64: {{ .EncryptionKey | printf "%q" }}
  cluster-backend-auth-token: {{ .BackendToken | printf "%q" }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: myceld-config
  namespace: {{ .Namespace }}
data:
  MYCELD_MODE: standalone
  MYCELD_GRPC_ADDR: 0.0.0.0:9091
  MYCELD_CLUSTER_NAME: {{ .ClusterName | printf "%q" }}
  MYCELD_CLUSTER_RAFT_NODE_COUNT: "{{ .NodeCount }}"
  MYCELD_CLUSTER_RAFT_REPLICA_FACTOR: "{{ .NodeCount }}"
  MYCELD_CLUSTER_RAFT_PARTITION_COUNT: "{{ .PartitionCount }}"
  MYCELD_CLUSTER_RAFT_NODE_ADDRS: {{ .NodeAddrs | printf "%q" }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ .HeadlessService }}
  namespace: {{ .Namespace }}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: {{ .SelectorApp }}
  ports:
    - name: grpc
      port: 9091
      targetPort: 9091
---
apiVersion: v1
kind: Service
metadata:
  name: {{ .Service }}
  namespace: {{ .Namespace }}
spec:
  selector:
    app: {{ .SelectorApp }}
  ports:
    - name: grpc
      port: 9091
      targetPort: 9091
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ .StatefulSet }}
  namespace: {{ .Namespace }}
spec:
  serviceName: {{ .HeadlessService }}
  podManagementPolicy: Parallel
  replicas: {{ .NodeCount }}
  selector:
    matchLabels:
      app: {{ .SelectorApp }}
  template:
    metadata:
      labels:
        app: {{ .SelectorApp }}
    spec:
      containers:
        - name: myceld
          image: {{ .Image }}
          imagePullPolicy: IfNotPresent
          command: ["/bin/sh", "-c"]
          args:
            - |
              ordinal="${HOSTNAME##*-}"
              export MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID="$((ordinal + 1))"
              export MYCELD_NODE_NAME="$HOSTNAME"
              export MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$HOSTNAME.{{ .HeadlessService }}.{{ .Namespace }}.svc.cluster.local:9091"
              exec myceld
          ports:
            - name: grpc
              containerPort: 9091
          envFrom:
            - configMapRef:
                name: myceld-config
          env:
            - name: MYCELD_DATA_DIR
              value: /data/mycel
            - name: MYCELD_BOOTSTRAP_ADMIN_USERNAME
              valueFrom:
                secretKeyRef:
                  name: myceld-secret
                  key: bootstrap-admin-username
            - name: MYCELD_BOOTSTRAP_ADMIN_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: myceld-secret
                  key: bootstrap-admin-password
            - name: MYCELD_USER_STORE_ENCRYPTION_KEY_B64
              valueFrom:
                secretKeyRef:
                  name: myceld-secret
                  key: user-store-encryption-key-b64
            - name: MYCELD_CLUSTER_BACKEND_AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: myceld-secret
                  key: cluster-backend-auth-token
          readinessProbe:
            exec:
              command:
                - /bin/sh
                - -c
                - |
                  mycel --daemon-addr 127.0.0.1:9091 --username "$MYCELD_BOOTSTRAP_ADMIN_USERNAME" --password "$MYCELD_BOOTSTRAP_ADMIN_PASSWORD" cluster readiness check >/tmp/mycel-readiness.log 2>&1
            initialDelaySeconds: 3
            periodSeconds: 2
            timeoutSeconds: 10
            failureThreshold: 60
          livenessProbe:
            tcpSocket:
              port: 9091
            initialDelaySeconds: 10
            periodSeconds: 10
          volumeMounts:
            - name: myceld-data
              mountPath: /data/mycel
  volumeClaimTemplates:
    - metadata:
        name: myceld-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
`
