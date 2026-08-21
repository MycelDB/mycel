package disrupttest

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type recordedCall struct {
	Name string
	Args []string
}

type fakeRunner struct {
	calls []recordedCall
	out   map[string]string
	err   error
}

func (f *fakeRunner) Run(ctx context.Context, name string, args ...string) (CommandResult, error) {
	f.calls = append(f.calls, recordedCall{Name: name, Args: append([]string(nil), args...)})
	if f.err != nil {
		return CommandResult{}, f.err
	}
	return CommandResult{Stdout: f.out[name+" "+strings.Join(args, " ")]}, nil
}

func TestK3DProvisionerCommandSequence(t *testing.T) {
	runner := &fakeRunner{}
	p := NewK3DProvisioner("cluster-a", runner)
	if err := p.Preflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, err := p.Create(context.Background(), ClusterConfig{Name: "cluster-a"})
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Name != "k3d-cluster-a" {
		t.Fatalf("kube context = %q", ctx.Name)
	}
	if err := p.LoadImage(context.Background(), "mycel:test"); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, call := range runner.calls {
		got = append(got, call.Name+" "+strings.Join(call.Args, " "))
	}
	want := []string{
		"kubectl version --client=true",
		"k3d version",
		"k3d cluster create cluster-a --agents 3 --wait --timeout 180s",
		"k3d image import mycel:test -c cluster-a",
		"k3d cluster delete cluster-a",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
}
