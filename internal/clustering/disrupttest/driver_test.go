package disrupttest

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestK3SDriverNodesParsesKubectlOutput(t *testing.T) {
	runner := &fakeRunner{out: map[string]string{
		"kubectl --context ctx -n ns get pods -l app=myceld -o jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}": "myceld-0\nmyceld-1\n",
	}}
	d := NewK3SDriver("ctx", "ns", "app=myceld", "myceld", "myceld", runner)
	nodes, err := d.Nodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nodes, []NodeRef{{Name: "myceld-0"}, {Name: "myceld-1"}}) {
		t.Fatalf("nodes = %#v", nodes)
	}
}

func TestK3SDriverApplyManifestsRunsApplyAndWait(t *testing.T) {
	runner := &fakeRunner{out: map[string]string{
		"kubectl --context ctx -n ns get statefulset myceld -o jsonpath={.spec.replicas}":                                "2",
		"kubectl --context ctx -n ns get pods -l app=myceld -o jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}": "myceld-0\nmyceld-1\n",
	}}
	d := NewK3SDriver("ctx", "ns", "app=myceld", "myceld", "myceld", runner)
	if err := d.ApplyManifests(context.Background(), "apiVersion: v1\nkind: Namespace\n"); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, call := range runner.calls {
		got = append(got, call.Name+" "+strings.Join(call.Args, " "))
	}
	if len(got) != 4 || !strings.Contains(got[0], "kubectl --context ctx apply -f") || got[1] != "kubectl --context ctx -n ns get statefulset myceld -o jsonpath={.spec.replicas}" || got[2] != "kubectl --context ctx -n ns get pods -l app=myceld -o jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}" || got[3] != "kubectl --context ctx -n ns wait --for=condition=Ready pod -l app=myceld --timeout=10m" {
		t.Fatalf("unexpected calls: %#v", got)
	}
}

func TestK3SDriverCollectArtifactsWritesFiles(t *testing.T) {
	dir := t.TempDir()
	runner := &fakeRunner{out: map[string]string{
		"kubectl --context ctx -n ns get pods -o wide":               "pods",
		"kubectl --context ctx -n ns get svc -o wide":                "services",
		"kubectl --context ctx -n ns get statefulset myceld -o yaml": "statefulset",
	}}
	d := NewK3SDriver("ctx", "ns", "app=myceld", "myceld", "myceld", runner)
	if err := d.CollectArtifacts(context.Background(), dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "pods.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "pods" {
		t.Fatalf("pods artifact = %q", data)
	}
}
