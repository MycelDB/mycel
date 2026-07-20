package wal

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Options struct {
	Dir          string
	SegmentBytes int64
}

type Manager struct {
	mu           sync.Mutex
	cond         *sync.Cond
	dir          string
	segmentBytes int64
	file         *os.File
	segmentStart LSN
	segmentSize  int64
	last         LSN
}

func Open(ctx context.Context, opts Options) (*Manager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.Dir == "" {
		return nil, fmt.Errorf("%w: dir is required", ErrInvalidRecord)
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 * 1024 * 1024
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, err
	}
	m := &Manager{dir: opts.Dir, segmentBytes: opts.SegmentBytes}
	m.cond = sync.NewCond(&m.mu)
	if err := m.scanAndOpen(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) Append(ctx context.Context, pending PendingRecord) (LSN, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lsn := m.last.Next()
	rec := Record{LSN: lsn, Type: pending.Type, SchemaVersion: pending.SchemaVersion, Timestamp: pending.Timestamp, Encoding: pending.Encoding, Payload: pending.Payload}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
	frame, err := encodeFrame(rec)
	if err != nil {
		return 0, err
	}
	if m.file == nil || (m.segmentSize > 0 && m.segmentSize+int64(len(frame)) > m.segmentBytes) {
		if err := m.rotateLocked(lsn); err != nil {
			return 0, err
		}
	}
	n, err := m.file.Write(frame)
	if err != nil {
		return 0, err
	}
	if n != len(frame) {
		return 0, io.ErrShortWrite
	}
	m.segmentSize += int64(n)
	m.last = lsn
	m.cond.Broadcast()
	return lsn, nil
}

func (m *Manager) Sync(ctx context.Context, lsn LSN) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if lsn > m.last {
		return fmt.Errorf("%w: lsn not committed", ErrInvalidRecord)
	}
	if m.file == nil {
		return nil
	}
	return m.file.Sync()
}

func (m *Manager) LastCommittedLSN() LSN { m.mu.Lock(); defer m.mu.Unlock(); return m.last }

func (m *Manager) WaitUntilCommitted(ctx context.Context, target LSN) error {
	if target == 0 {
		return nil
	}
	done := make(chan struct{})
	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for m.last < target {
			m.cond.Wait()
		}
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

type RetainedRange struct {
	FirstRetainedLSN LSN
	LastCommittedLSN LSN
}

func (m *Manager) RetainedRange(ctx context.Context) (RetainedRange, error) {
	if err := ctx.Err(); err != nil {
		return RetainedRange{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	first, err := m.oldestRetainedLSNLocked()
	if err != nil {
		return RetainedRange{}, err
	}
	return RetainedRange{FirstRetainedLSN: first, LastCommittedLSN: m.last}, nil
}

func (m *Manager) Status() (Status, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldest, err := m.oldestRetainedLSNLocked()
	if err != nil {
		return Status{}, err
	}
	return Status{LastCommittedLSN: m.last, OldestRetainedLSN: oldest, CurrentSegmentStart: m.segmentStart, CurrentSegmentBytes: m.segmentSize}, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

func (m *Manager) ReadFrom(ctx context.Context, lsn LSN) (*Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	segments, err := listSegments(m.dir)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return newIterator(ctx, nil, lsn), nil
	}
	idx := 0
	for i, s := range segments {
		if s.start <= lsn {
			idx = i
		}
	}
	return newIterator(ctx, segments[idx:], lsn), nil
}

func (m *Manager) scanAndOpen() error {
	segments, err := listSegments(m.dir)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return m.rotateLocked(1)
	}
	var last LSN
	for si, seg := range segments {
		f, err := os.OpenFile(seg.path, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		off := int64(0)
		for {
			rec, n, st, err := decodeFrame(f)
			if err != nil {
				f.Close()
				return err
			}
			if st == frameEOF {
				break
			}
			if st == frameTorn {
				if si != len(segments)-1 {
					f.Close()
					return fmt.Errorf("%w: torn frame before final segment", ErrCorrupt)
				}
				if err := f.Truncate(off); err != nil {
					f.Close()
					return err
				}
				break
			}
			if rec.LSN != last.Next() {
				f.Close()
				return fmt.Errorf("%w: non-contiguous lsn", ErrCorrupt)
			}
			last = rec.LSN
			off += n
		}
		f.Close()
	}
	m.last = last
	lastSeg := segments[len(segments)-1]
	f, err := os.OpenFile(lastSeg.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	m.file = f
	m.segmentStart = lastSeg.start
	m.segmentSize = info.Size()
	return nil
}

func (m *Manager) rotateLocked(start LSN) error {
	if m.file != nil {
		if err := m.file.Close(); err != nil {
			return err
		}
	}
	path := filepath.Join(m.dir, segmentName(start))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0o600)
	if os.IsExist(err) {
		f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	}
	if err != nil {
		return err
	}
	m.file = f
	m.segmentStart = start
	m.segmentSize = 0
	return nil
}

type segmentInfo struct {
	start LSN
	path  string
}

func segmentName(start LSN) string { return fmt.Sprintf("%016d.wal", uint64(start)) }

func listSegments(dir string) ([]segmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := []segmentInfo{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		n := strings.TrimSuffix(e.Name(), ".wal")
		u, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: bad segment name %s", ErrCorrupt, e.Name())
		}
		out = append(out, segmentInfo{start: LSN(u), path: filepath.Join(dir, e.Name())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out, nil
}
