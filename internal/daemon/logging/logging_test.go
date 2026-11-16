package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/myceldb/mycel/internal/fsperm"
)

func TestRollingFileRotatesAtSizeBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "myceld.log")
	file, err := newRollingFile(path, 10, 2)
	if err != nil {
		t.Fatalf("newRollingFile() error = %v", err)
	}
	defer file.Close()

	if _, err := file.Write([]byte("1234567890")); err != nil {
		t.Fatalf("write boundary record: %v", err)
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatalf("unexpected rotation at exact boundary: %v", err)
	}
	if _, err := file.Write([]byte("x")); err != nil {
		t.Fatalf("write record beyond boundary: %v", err)
	}

	if got := readLogFile(t, path+".1"); got != "1234567890" {
		t.Fatalf("rotated log = %q, want boundary record", got)
	}
	if got := readLogFile(t, path); got != "x" {
		t.Fatalf("active log = %q, want new record", got)
	}
	for _, logPath := range []string{path, path + ".1"} {
		info, err := os.Stat(logPath)
		if err != nil {
			t.Fatalf("stat %s: %v", logPath, err)
		}
		if got := info.Mode().Perm(); got != fsperm.PrivateFile {
			t.Fatalf("permissions for %s = %o, want %o", logPath, got, fsperm.PrivateFile)
		}
	}
}

func TestRollingFileRetainsNumericRotations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "myceld.log")
	file, err := newRollingFile(path, 3, 2)
	if err != nil {
		t.Fatalf("newRollingFile() error = %v", err)
	}
	defer file.Close()

	for _, record := range []string{"aaa", "bbb", "ccc", "ddd"} {
		if _, err := file.Write([]byte(record)); err != nil {
			t.Fatalf("write %q: %v", record, err)
		}
	}

	if got := readLogFile(t, path); got != "ddd" {
		t.Fatalf("active log = %q, want ddd", got)
	}
	if got := readLogFile(t, path+".1"); got != "ccc" {
		t.Fatalf("newest rotated log = %q, want ccc", got)
	}
	if got := readLogFile(t, path+".2"); got != "bbb" {
		t.Fatalf("oldest retained log = %q, want bbb", got)
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("unexpected log beyond retention: %v", err)
	}
}

func TestRollingFileRotatesOversizedActiveFileOnStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "myceld.log")
	if err := os.WriteFile(path, []byte("oversized"), fsperm.PrivateFile); err != nil {
		t.Fatalf("seed active log: %v", err)
	}

	file, err := newRollingFile(path, 4, 2)
	if err != nil {
		t.Fatalf("newRollingFile() error = %v", err)
	}
	defer file.Close()

	if got := readLogFile(t, path+".1"); got != "oversized" {
		t.Fatalf("startup rotation = %q, want oversized active log", got)
	}
	if got := readLogFile(t, path); got != "" {
		t.Fatalf("active log after startup rotation = %q, want empty", got)
	}
	if _, err := file.Write([]byte("new")); err != nil {
		t.Fatalf("write after startup rotation: %v", err)
	}
	if got := readLogFile(t, path); got != "new" {
		t.Fatalf("active log after write = %q, want new", got)
	}
}

func TestRollingFileSerializesConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "myceld.log")
	file, err := newRollingFile(path, 64, 20)
	if err != nil {
		t.Fatalf("newRollingFile() error = %v", err)
	}
	defer file.Close()

	const records = 100
	var wg sync.WaitGroup
	for i := 0; i < records; i++ {
		record := fmt.Sprintf("record-%03d\n", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := file.Write([]byte(record)); err != nil {
				t.Errorf("concurrent write %q: %v", record, err)
			}
		}()
	}
	wg.Wait()

	var contents strings.Builder
	contents.WriteString(readLogFile(t, path))
	for index := 1; index <= 20; index++ {
		rotated := path + fmt.Sprintf(".%d", index)
		if _, err := os.Stat(rotated); os.IsNotExist(err) {
			continue
		}
		contents.WriteString(readLogFile(t, rotated))
	}
	for i := 0; i < records; i++ {
		record := fmt.Sprintf("record-%03d\n", i)
		if got := strings.Count(contents.String(), record); got != 1 {
			t.Fatalf("record %q appears %d times, want once", record, got)
		}
	}
}

func readLogFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}
