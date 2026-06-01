package client

import (
	"context"
	"path/filepath"
	"testing"
)

func TestOpenEngine_StandaloneSuccess(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "knotdb")

	engine, err := OpenEngine(EngineConfig{
		DataDir:         dataDir,
		Mode:            EngineModeStandalone,
		CreateIfMissing: true,
	})
	if err != nil {
		t.Fatalf("expected engine open success, got error: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	if err := engine.Ready(context.Background()); err != nil {
		t.Fatalf("expected engine ready, got error: %v", err)
	}
}
