package client

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestQueryServiceAdapterHasNoGraphExecutionOrLegacyBridgeDependencies(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate query adapter boundary test source")
	}
	dir := filepath.Dir(testFile)
	queryFiles, err := filepath.Glob(filepath.Join(dir, "query*.go"))
	if err != nil {
		t.Fatalf("find query adapter sources: %v", err)
	}
	for _, file := range queryFiles {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s imports: %v", filepath.Base(file), err)
		}
		for _, spec := range parsed.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("decode import %s: %v", spec.Path.Value, err)
			}
			for _, forbidden := range []string{
				"github.com/myceldb/mycel/internal/graph/query",
				"github.com/myceldb/mycel/internal/query/execution",
				"github.com/myceldb/mycel/internal/query/gql/execution",
			} {
				if strings.HasPrefix(imported, forbidden) {
					t.Errorf("query API source %s imports execution dependency %q", filepath.Base(file), imported)
				}
			}
		}
	}

	serviceFile := filepath.Join(dir, "query_service.go")
	source, err := os.ReadFile(serviceFile)
	if err != nil {
		t.Fatalf("read query service: %v", err)
	}
	for _, forbidden := range []string{
		"ListNodes(", "ListEdges(", "ScanNodes(", "ScanLabel(", "ScanAdjacency(",
		"tryExecuteIndexed", "queryExecution", "queryRowState", "LegacyBridge",
	} {
		if strings.Contains(string(source), forbidden) {
			t.Errorf("query API adapter contains execution-owned symbol %q", forbidden)
		}
	}

	for _, removed := range []string{"query_engine_legacy_source.go", "query_service_schema_structured.go"} {
		if _, err := os.Stat(filepath.Join(dir, removed)); !os.IsNotExist(err) {
			t.Errorf("removed query adapter bridge %s still exists", removed)
		}
	}
}
