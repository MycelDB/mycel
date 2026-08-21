package disrupttest

import (
	"context"
	"fmt"
)

type KubeContext struct {
	Name string
}

type ClusterProvisioner interface {
	Name() string
	Preflight(ctx context.Context) error
	Create(ctx context.Context, cfg ClusterConfig) (KubeContext, error)
	LoadImage(ctx context.Context, image string) error
	Delete(ctx context.Context) error
}

type K3DProvisioner struct {
	ClusterName string
	Runner      CommandRunner
}

func NewK3DProvisioner(clusterName string, runner CommandRunner) *K3DProvisioner {
	if runner == nil {
		runner = ExecRunner{}
	}
	return &K3DProvisioner{ClusterName: clusterName, Runner: runner}
}

func (p *K3DProvisioner) Name() string { return "k3d" }

func (p *K3DProvisioner) Preflight(ctx context.Context) error {
	if _, err := p.Runner.Run(ctx, "kubectl", "version", "--client=true"); err != nil {
		return fmt.Errorf("kubectl preflight failed: %w", err)
	}
	if _, err := p.Runner.Run(ctx, "k3d", "version"); err != nil {
		return fmt.Errorf("k3d preflight failed: %w", err)
	}
	return nil
}

func (p *K3DProvisioner) Create(ctx context.Context, cfg ClusterConfig) (KubeContext, error) {
	name := cfg.Name
	if name == "" {
		name = p.ClusterName
	}
	if name == "" {
		return KubeContext{}, fmt.Errorf("cluster name is required")
	}
	args := []string{"cluster", "create", name, "--agents", "3", "--wait", "--timeout", "180s"}
	if _, err := p.Runner.Run(ctx, "k3d", args...); err != nil {
		return KubeContext{}, err
	}
	return KubeContext{Name: "k3d-" + name}, nil
}

func (p *K3DProvisioner) LoadImage(ctx context.Context, image string) error {
	if image == "" {
		return fmt.Errorf("image is required")
	}
	if p.ClusterName == "" {
		return fmt.Errorf("cluster name is required")
	}
	_, err := p.Runner.Run(ctx, "k3d", "image", "import", image, "-c", p.ClusterName)
	return err
}

func (p *K3DProvisioner) Delete(ctx context.Context) error {
	if p.ClusterName == "" {
		return nil
	}
	_, err := p.Runner.Run(ctx, "k3d", "cluster", "delete", p.ClusterName)
	return err
}
