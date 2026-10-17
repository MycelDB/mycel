package engine

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestEngineProductionCodeDoesNotImportTransportDaemonOrServiceImplementations(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate engine import-boundary test source")
	}
	engineDir := filepath.Dir(testFile)
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
	err := filepath.WalkDir(engineDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			for _, prefix := range forbidden {
				if strings.HasPrefix(importPath, prefix) {
					t.Errorf("query engine production file %s imports forbidden package %q", path, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan engine imports: %v", err)
	}
}
