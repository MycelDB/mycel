package cmd

import (
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/cli/app"
)

func TestGraphBenchmarkWritesCommandShape(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	cmd, _, err := root.Find([]string{"graph", "benchmark", "writes"})
	if err != nil {
		t.Fatalf("Find graph benchmark writes: %v", err)
	}
	if cmd == nil || cmd.Use != "writes" {
		t.Fatalf("command = %#v, want writes", cmd)
	}
	for _, flag := range []string{"space-id", "domain-id", "domain", "graph-size", "seed-nodes", "operations", "batch-size", "operation", "template", "seed-template", "references-per-node", "reference-labels", "apply-operations", "cleanup"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Fatalf("missing --%s flag", flag)
		}
	}
}

func TestGraphBenchmarkSeedTarget(t *testing.T) {
	cases := []struct {
		name     string
		size     string
		override int
		want     int
	}{
		{name: "small", size: "small", want: graphBenchmarkDefaultSmall},
		{name: "medium", size: "medium", want: graphBenchmarkDefaultMedium},
		{name: "large", size: "large", want: graphBenchmarkDefaultLarge},
		{name: "custom", size: "custom", want: 0},
		{name: "override", size: "small", override: 42, want: 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := graphBenchmarkSeedTarget(tc.size, tc.override)
			if err != nil {
				t.Fatalf("graphBenchmarkSeedTarget error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("target = %d, want %d", got, tc.want)
			}
		})
	}
	if _, err := graphBenchmarkSeedTarget("huge", 0); err == nil {
		t.Fatal("expected unsupported graph size error")
	}
}

func TestSummarizeDurations(t *testing.T) {
	summary := summarizeDurations([]time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond})
	if summary.Count != 4 || summary.MinMS != 10 || summary.P50MS != 20 || summary.P95MS != 40 || summary.P99MS != 40 || summary.MaxMS != 40 || summary.AvgMS != 25 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestBenchmarkNodeShapeTemplates(t *testing.T) {
	for _, template := range []string{"minimal", "properties", "content", "commonfolio-session", "commonfolio-journal", "commonfolio-journal-entry", "commonfolio-project", "commonfolio-task", "audit"} {
		node, err := benchmarkNodeShape(template, "run", 1, "test")
		if err != nil {
			t.Fatalf("benchmarkNodeShape(%s) error = %v", template, err)
		}
		if len(node.GetLabels()) == 0 {
			t.Fatalf("benchmarkNodeShape(%s) has no labels", template)
		}
		if node.GetProperties() == nil || node.GetPayload() == nil {
			t.Fatalf("benchmarkNodeShape(%s) missing properties/payload", template)
		}
	}
	if _, err := benchmarkNodeShape("unknown", "run", 1, "test"); err == nil {
		t.Fatal("expected unsupported template error")
	}
}

func TestBenchmarkReferenceLabels(t *testing.T) {
	got := benchmarkReferenceLabels("belongs_to, references,belongs_to,,parent")
	want := []string{"belongs_to", "references", "parent"}
	if len(got) != len(want) {
		t.Fatalf("labels = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("labels = %#v, want %#v", got, want)
		}
	}
}
