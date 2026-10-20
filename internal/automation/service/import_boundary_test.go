package service

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

func TestAutomationProductionCodeDoesNotImportSiblingServices(t *testing.T) {
	const internalPrefix = "github.com/myceldb/mycel/internal/"

	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate automation import-boundary test source")
	}
	automationDir := filepath.Dir(filepath.Dir(testFile))
	err := filepath.WalkDir(automationDir, func(path string, entry fs.DirEntry, walkErr error) error {
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
			if !strings.HasPrefix(importPath, internalPrefix) {
				continue
			}
			for _, segment := range strings.Split(strings.TrimPrefix(importPath, internalPrefix), "/") {
				if segment == "service" {
					t.Errorf("automation production file %s imports sibling service package %q", path, importPath)
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan automation imports: %v", err)
	}
}
