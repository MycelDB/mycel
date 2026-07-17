package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/myceldb/mycel/internal/wal"
)

type ReceiveLog struct{ dir string }

func NewReceiveLog(dir string) *ReceiveLog { return &ReceiveLog{dir: dir} }

func (l *ReceiveLog) Put(ctx context.Context, rec Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec.LSN == wal.ZeroLSN {
		return fmt.Errorf("lsn must be non-zero")
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		return err
	}
	path := l.path(rec.LSN)
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, raw) {
			return nil
		}
		var old Record
		if json.Unmarshal(existing, &old) == nil && recordsEqual(old, rec) {
			return nil
		}
		return fmt.Errorf("receive log conflict at lsn %s", rec.LSN)
	} else if !os.IsNotExist(err) {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (l *ReceiveLog) Get(ctx context.Context, lsn wal.LSN) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	raw, err := os.ReadFile(l.path(lsn))
	if err != nil {
		return Record{}, err
	}
	var rec Record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}

func (l *ReceiveLog) ScanAfter(ctx context.Context, after wal.LSN) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(l.dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var lsns []wal.LSN
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".json"), 10, 64)
		if err != nil {
			return nil, err
		}
		if wal.LSN(n) > after {
			lsns = append(lsns, wal.LSN(n))
		}
	}
	sort.Slice(lsns, func(i, j int) bool { return lsns[i] < lsns[j] })
	out := make([]Record, 0, len(lsns))
	for _, lsn := range lsns {
		rec, err := l.Get(ctx, lsn)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func (l *ReceiveLog) Clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.RemoveAll(l.dir); err != nil {
		return err
	}
	return os.MkdirAll(l.dir, 0o700)
}

func (l *ReceiveLog) TruncateBefore(ctx context.Context, before wal.LSN) error {
	entries, err := os.ReadDir(l.dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".json"), 10, 64)
		if err != nil {
			return err
		}
		if wal.LSN(n) < before {
			if err := os.Remove(filepath.Join(l.dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *ReceiveLog) path(lsn wal.LSN) string {
	return filepath.Join(l.dir, fmt.Sprintf("%020d.json", uint64(lsn)))
}

func recordsEqual(a, b Record) bool {
	return a.LSN == b.LSN && a.Type == b.Type && a.SchemaVersion == b.SchemaVersion && a.Timestamp.Equal(b.Timestamp) && a.Encoding == b.Encoding && bytes.Equal(a.Payload, b.Payload)
}
