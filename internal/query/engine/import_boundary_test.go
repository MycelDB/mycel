package engine

import (
	"go/build"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestEngineProductionCodeDoesNotImportTransportDaemonOrServiceImplementations(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate engine import-boundary test source")
	}
	engineDir := filepath.Dir(testFile)
	pkg, err := build.Default.ImportDir(engineDir, build.IgnoreVendor)
	if err != nil {
		t.Fatalf("load engine package imports: %v", err)
	}
	forbidden := []string{
		"github.com/myceldb/mycel/internal/daemon",
		"github.com/myceldb/mycel/internal/gen",
		"github.com/myceldb/mycel/internal/graph/service",
		"github.com/myceldb/mycel/internal/schema/service",
		"github.com/myceldb/mycel/internal/semantic/service",
		"github.com/myceldb/mycel/internal/session/service",
		"github.com/myceldb/mycel/internal/space/service",
		"google.golang.org/protobuf",
	}
	for _, imported := range pkg.Imports {
		for _, prefix := range forbidden {
			if strings.HasPrefix(imported, prefix) {
				t.Errorf("query engine imports forbidden package %q", imported)
			}
		}
	}
}
