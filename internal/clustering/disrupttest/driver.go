package disrupttest

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type NodeRef struct {
	Name string
}

type Endpoint struct {
	Addr string
}

type ClusterDriver interface {
	Name() string
	ApplyManifests(ctx context.Context, manifests string) error
	Nodes(ctx context.Context) ([]NodeRef, error)
	WaitReady(ctx context.Context, node NodeRef) error
	WaitAllReady(ctx context.Context) error
	RestartNode(ctx context.Context, node NodeRef) error
	PortForward(ctx context.Context, node NodeRef, port int) (Endpoint, func(), error)
	ServiceEndpoint(ctx context.Context) (Endpoint, func(), error)
	CollectArtifacts(ctx context.Context, dir string) error
}

type K3SDriver struct {
	KubeContext string
	Namespace   string
	Selector    string
	Service     string
	StatefulSet string
	Runner      CommandRunner
}

func NewK3SDriver(kubeContext, namespace, selector, service, statefulSet string, runner CommandRunner) *K3SDriver {
	if runner == nil {
		runner = ExecRunner{}
	}
	return &K3SDriver{KubeContext: kubeContext, Namespace: namespace, Selector: selector, Service: service, StatefulSet: statefulSet, Runner: runner}
}

func (d *K3SDriver) Name() string { return "k3s" }

func (d *K3SDriver) kubectlArgs(args ...string) []string {
	out := []string{}
	if strings.TrimSpace(d.KubeContext) != "" {
		out = append(out, "--context", strings.TrimSpace(d.KubeContext))
	}
	return append(out, args...)
}

func (d *K3SDriver) ApplyManifests(ctx context.Context, manifests string) error {
	tmp, err := os.CreateTemp("", "mycel-raft-disrupt-*.yaml")
	if err != nil {
		return err
	}
	path := tmp.Name()
	defer os.Remove(path)
	if _, err := tmp.WriteString(manifests); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if _, err := d.Runner.Run(ctx, "kubectl", d.kubectlArgs("apply", "-f", path)...); err != nil {
		return err
	}
	return d.WaitAllReady(ctx)
}

func (d *K3SDriver) Nodes(ctx context.Context) ([]NodeRef, error) {
	res, err := d.Runner.Run(ctx, "kubectl", d.kubectlArgs("-n", d.Namespace, "get", "pods", "-l", d.Selector, "-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`)...)
	if err != nil {
		return nil, err
	}
	var nodes []NodeRef
	for _, line := range strings.Split(res.Stdout, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			nodes = append(nodes, NodeRef{Name: line})
		}
	}
	return nodes, nil
}

func (d *K3SDriver) WaitSystemReady(ctx context.Context) error {
	deadline := time.Now().Add(5 * time.Minute)
	nextLog := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := d.waitSystemDeployment(ctx, "coredns"); err != nil {
			lastErr = err
		} else if err := d.waitSystemDeployment(ctx, "local-path-provisioner"); err != nil {
			lastErr = err
		} else {
			return nil
		}
		if time.Now().After(nextLog) {
			fmt.Fprintf(os.Stderr, "[raft-disrupt] still waiting for k3s system readiness; last error: %s\n", summarizeError(lastErr, 300))
			nextLog = time.Now().Add(15 * time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timed out waiting for k3s system readiness: %w", lastErr)
}

func (d *K3SDriver) waitSystemDeployment(ctx context.Context, name string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err := d.Runner.Run(cmdCtx, "kubectl", d.kubectlArgs("--request-timeout=5s", "-n", "kube-system", "wait", "--for=condition=Available", "deployment/"+name, "--timeout=15s")...)
	return err
}

func (d *K3SDriver) WaitReady(ctx context.Context, node NodeRef) error {
	_, err := d.Runner.Run(ctx, "kubectl", d.kubectlArgs("-n", d.Namespace, "wait", "--for=condition=Ready", "pod/"+node.Name, "--timeout=10m")...)
	return err
}

func (d *K3SDriver) WaitAllReady(ctx context.Context) error {
	desired := 1
	if d.StatefulSet != "" {
		if replicas, err := d.desiredReplicas(ctx); err != nil {
			return err
		} else if replicas > 0 {
			desired = replicas
		}
	}
	deadline := time.Now().Add(10 * time.Minute)
	nextLog := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		nodes, err := d.Nodes(ctx)
		if err != nil {
			lastErr = err
		} else if len(nodes) < desired {
			lastErr = fmt.Errorf("waiting for %d pods matching %s; found %d", desired, d.Selector, len(nodes))
		} else {
			_, err := d.Runner.Run(ctx, "kubectl", d.kubectlArgs("-n", d.Namespace, "wait", "--for=condition=Ready", "pod", "-l", d.Selector, "--timeout=10m")...)
			return err
		}
		if time.Now().After(nextLog) {
			fmt.Fprintf(os.Stderr, "[raft-disrupt] still waiting for mycel pods to exist; last status: %s\n", summarizeError(lastErr, 300))
			nextLog = time.Now().Add(15 * time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timed out waiting for mycel pods to exist: %w", lastErr)
}

func (d *K3SDriver) desiredReplicas(ctx context.Context) (int, error) {
	res, err := d.Runner.Run(ctx, "kubectl", d.kubectlArgs("-n", d.Namespace, "get", "statefulset", d.StatefulSet, "-o", "jsonpath={.spec.replicas}")...)
	if err != nil {
		return 0, err
	}
	text := strings.TrimSpace(res.Stdout)
	if text == "" {
		return 0, nil
	}
	replicas, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("parse statefulset replicas %q: %w", text, err)
	}
	return replicas, nil
}

func (d *K3SDriver) RestartNode(ctx context.Context, node NodeRef) error {
	if node.Name == "" {
		return fmt.Errorf("node name is required")
	}
	if _, err := d.Runner.Run(ctx, "kubectl", d.kubectlArgs("-n", d.Namespace, "delete", "pod", node.Name, "--wait=true", "--timeout=3m")...); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		if err := d.WaitReady(ctx, node); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timed out waiting for restarted pod %s", node.Name)
}

func (d *K3SDriver) PortForward(ctx context.Context, node NodeRef, port int) (Endpoint, func(), error) {
	if node.Name == "" {
		return Endpoint{}, func() {}, fmt.Errorf("node name is required")
	}
	return d.startPortForward(ctx, "pod/"+node.Name, port)
}

func (d *K3SDriver) ServiceEndpoint(ctx context.Context) (Endpoint, func(), error) {
	if d.Service == "" {
		return Endpoint{}, func() {}, fmt.Errorf("service name is required")
	}
	return d.startPortForward(ctx, "svc/"+d.Service, 9091)
}

func (d *K3SDriver) startPortForward(ctx context.Context, target string, remotePort int) (Endpoint, func(), error) {
	localPort, err := reserveLocalPort()
	if err != nil {
		return Endpoint{}, func() {}, err
	}
	pfCtx, cancel := context.WithCancel(ctx)
	args := d.kubectlArgs("-n", d.Namespace, "port-forward", target, fmt.Sprintf("%d:%d", localPort, remotePort))
	cmd := exec.CommandContext(pfCtx, "kubectl", args...)
	if envBool("MYCEL_DISRUPT_DEBUG_PORT_FORWARD", false) {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return Endpoint{}, func() {}, err
	}
	cleanup := func() {
		cancel()
		_ = cmd.Wait()
	}
	addr := fmt.Sprintf("127.0.0.1:%d", localPort)
	if err := waitTCP(ctx, addr, 30*time.Second); err != nil {
		cleanup()
		return Endpoint{}, func() {}, err
	}
	return Endpoint{Addr: addr}, cleanup, nil
}

func summarizeError(err error, limit int) string {
	if err == nil {
		return ""
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	if limit > 0 && len(msg) > limit {
		return msg[:limit] + "..."
	}
	return msg
}

func reserveLocalPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func waitTCP(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		dialer := net.Dialer{Timeout: time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out waiting for port-forward %s", addr)
}

func (d *K3SDriver) CollectArtifacts(ctx context.Context, dir string) error {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	commands := map[string][]string{
		"pods.txt":        d.kubectlArgs("-n", d.Namespace, "get", "pods", "-o", "wide"),
		"services.txt":    d.kubectlArgs("-n", d.Namespace, "get", "svc", "-o", "wide"),
		"statefulset.txt": d.kubectlArgs("-n", d.Namespace, "get", "statefulset", d.StatefulSet, "-o", "yaml"),
	}
	for name, args := range commands {
		res, err := d.Runner.Run(ctx, "kubectl", args...)
		content := res.Stdout
		if err != nil {
			content += "\nERROR: " + err.Error() + "\n" + res.Stderr
		}
		if writeErr := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); writeErr != nil {
			return writeErr
		}
	}
	return nil
}
