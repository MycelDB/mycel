package engine

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestQueryEngineProductionCodeDoesNotImportTransportDaemonOrServiceImplementations(t *testing.T) {
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
	for _, dir := range []string{
		engineDir,
		filepath.Join(engineDir, "..", "application"),
		filepath.Join(engineDir, "..", "binding"),
		filepath.Join(engineDir, "..", "execution"),
		filepath.Join(engineDir, "..", "physical"),
		filepath.Join(engineDir, "..", "schema"),
	} {
		imports, err := productionImportPaths(dir)
		if err != nil {
			t.Fatalf("load query package imports from %s: %v", dir, err)
		}
		for _, imported := range imports {
			for _, prefix := range forbidden {
				if strings.HasPrefix(imported, prefix) {
					t.Errorf("query package %s imports forbidden package %q", dir, imported)
				}
			}
		}
	}
}

func TestProductionImportPathsIncludesBuildConstrainedFilesAndExcludesTests(t *testing.T) {
	dir := t.TempDir()
	buildConstrained := "//go:build windows\n\npackage sample\n\nimport _ \"example.com/production\"\n"
	if err := os.WriteFile(filepath.Join(dir, "windows.go"), []byte(buildConstrained), 0o600); err != nil {
		t.Fatalf("write constrained production source: %v", err)
	}
	testSource := "package sample\n\nimport _ \"example.com/test-only\"\n"
	if err := os.WriteFile(filepath.Join(dir, "sample_test.go"), []byte(testSource), 0o600); err != nil {
		t.Fatalf("write test source: %v", err)
	}

	imports, err := productionImportPaths(dir)
	if err != nil {
		t.Fatalf("parse production imports: %v", err)
	}
	if len(imports) != 1 || imports[0] != "example.com/production" {
		t.Fatalf("production imports = %v, want build-constrained import only", imports)
	}
}

// productionImportPaths parses source directly instead of using the active Go
// build context, so platform- and tag-constrained production files are covered.
func productionImportPaths(dir string) ([]string, error) {
	packages, err := parser.ParseDir(token.NewFileSet(), dir, func(info os.FileInfo) bool {
		return filepath.Ext(info.Name()) == ".go" && !strings.HasSuffix(info.Name(), "_test.go")
	}, parser.ImportsOnly)
	if err != nil {
		return nil, err
	}
	unique := map[string]struct{}{}
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, spec := range file.Imports {
				imported, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return nil, err
				}
				unique[imported] = struct{}{}
			}
		}
	}
	imports := make([]string, 0, len(unique))
	for imported := range unique {
		imports = append(imports, imported)
	}
	sort.Strings(imports)
	return imports, nil
}
