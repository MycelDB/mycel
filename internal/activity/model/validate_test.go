package model

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestNormalizeForAppendRequiresCuratedFields(t *testing.T) {
	_, err := NormalizeForAppend(Event{Severity: SeverityInfo, Category: CategoryLifecycle, Type: "daemon.started", Message: "started"}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "source is required") {
		t.Fatalf("expected source validation error, got %v", err)
	}
}

func TestNormalizeForAppendRejectsSecretMetadataKeys(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"password": "redacted?"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NormalizeForAppend(Event{Severity: SeverityInfo, Category: CategoryExternal, Type: "external.event", Message: "event", Source: Source{Service: "test"}, Metadata: metadata}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "metadata key") {
		t.Fatalf("expected metadata validation error, got %v", err)
	}
}

func TestNormalizeForAppendDefaultsTimestamps(t *testing.T) {
	now := time.Date(2026, 8, 18, 1, 2, 3, 0, time.UTC)
	event, err := NormalizeForAppend(Event{Severity: SeverityWarning, Category: CategoryCluster, Type: "cluster.degraded", Message: "cluster degraded", Source: Source{Component: "cluster"}}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !event.IngestedAt.Equal(now) || !event.OccurredAt.Equal(now) {
		t.Fatalf("timestamps not defaulted: %#v", event)
	}
}
