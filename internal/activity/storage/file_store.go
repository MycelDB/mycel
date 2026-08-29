package storage

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/activity/model"
)

const defaultPageSize = 50
const maxPageSize = 500

type FileStore struct {
	path          string
	mu            sync.RWMutex
	events        []model.Event
	byID          map[string]int
	byIdempotency map[string]string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path, byID: map[string]int{}, byIdempotency: map[string]string{}}
}

func (s *FileStore) Open(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 8*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event model.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return fmt.Errorf("decode activity event: %w", err)
		}
		s.events = append(s.events, event)
		s.byID[event.EventID] = len(s.events) - 1
		if key := idempotencyIndexKey(event); key != "" {
			s.byIdempotency[key] = event.EventID
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	s.sortLocked()
	return nil
}

func (s *FileStore) Append(ctx context.Context, event model.Event) (AppendResult, error) {
	select {
	case <-ctx.Done():
		return AppendResult{}, ctx.Err()
	default:
	}
	now := time.Now().UTC()
	normalized, err := model.NormalizeForAppend(event, now)
	if err != nil {
		return AppendResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if key := idempotencyIndexKey(normalized); key != "" {
		if existingID := s.byIdempotency[key]; existingID != "" {
			idx := s.byID[existingID]
			return AppendResult{Event: s.events[idx], Duplicate: true}, nil
		}
	}
	if normalized.EventID == "" {
		normalized.EventID = "evt_" + uuid.NewString()
	}
	if _, exists := s.byID[normalized.EventID]; exists {
		return AppendResult{}, fmt.Errorf("%w: duplicate event id", model.ErrInvalidEvent)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return AppendResult{}, err
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return AppendResult{}, err
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return AppendResult{}, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return AppendResult{}, err
	}
	if err := file.Close(); err != nil {
		return AppendResult{}, err
	}
	s.events = append(s.events, normalized)
	s.byID[normalized.EventID] = len(s.events) - 1
	if key := idempotencyIndexKey(normalized); key != "" {
		s.byIdempotency[key] = normalized.EventID
	}
	s.sortLocked()
	return AppendResult{Event: normalized}, nil
}

func (s *FileStore) Get(ctx context.Context, eventID string) (model.Event, error) {
	select {
	case <-ctx.Done():
		return model.Event{}, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.byID[strings.TrimSpace(eventID)]
	if !ok {
		return model.Event{}, model.ErrNotFound
	}
	return s.events[idx], nil
}

func (s *FileStore) List(ctx context.Context, filter model.ListFilter) (model.ListResult, error) {
	select {
	case <-ctx.Done():
		return model.ListResult{}, ctx.Err()
	default:
	}
	pageSize := filter.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	startAfter, err := decodePageToken(filter.PageToken)
	if err != nil {
		return model.ListResult{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Event, 0, pageSize)
	summary := model.ListSummary{}
	pageFilled := false
	hasMore := false
	for index, event := range s.events {
		if index%1024 == 0 {
			select {
			case <-ctx.Done():
				return model.ListResult{}, ctx.Err()
			default:
			}
		}
		if !matches(event, filter) {
			continue
		}
		summary.TotalCount++
		switch strings.ToLower(event.Severity) {
		case model.SeverityWarning:
			summary.WarningCount++
		case model.SeverityError:
			summary.ErrorCount++
		}
		pageEligible := startAfter.IsZero() || event.IngestedAt.Before(startAfter)
		if !pageEligible {
			continue
		}
		if pageFilled {
			hasMore = true
			continue
		}
		out = append(out, event)
		if len(out) == pageSize {
			pageFilled = true
		}
	}
	next := ""
	if hasMore && len(out) > 0 {
		next = encodePageToken(out[len(out)-1].IngestedAt)
	}
	return model.ListResult{Events: out, NextPageToken: next, Summary: summary}, nil
}

func (s *FileStore) sortLocked() {
	sort.SliceStable(s.events, func(i, j int) bool {
		if s.events[i].IngestedAt.Equal(s.events[j].IngestedAt) {
			return s.events[i].EventID > s.events[j].EventID
		}
		return s.events[i].IngestedAt.After(s.events[j].IngestedAt)
	})
	s.byID = map[string]int{}
	for i, event := range s.events {
		s.byID[event.EventID] = i
	}
}

func matches(event model.Event, filter model.ListFilter) bool {
	if !filter.Since.IsZero() && event.OccurredAt.Before(filter.Since) {
		return false
	}
	if !filter.Until.IsZero() && event.OccurredAt.After(filter.Until) {
		return false
	}
	if len(filter.Severities) > 0 && !contains(filter.Severities, event.Severity) {
		return false
	}
	if len(filter.Categories) > 0 && !contains(filter.Categories, event.Category) {
		return false
	}
	if len(filter.Types) > 0 && !contains(filter.Types, event.Type) {
		return false
	}
	if filter.SourceNodeID != "" && event.Source.NodeID != filter.SourceNodeID {
		return false
	}
	if filter.SourcePodName != "" && event.Source.PodName != filter.SourcePodName {
		return false
	}
	if filter.SourceComponent != "" && event.Source.Component != filter.SourceComponent {
		return false
	}
	if filter.SourceService != "" && event.Source.Service != filter.SourceService {
		return false
	}
	if filter.ActorPrincipalID != "" && event.Actor.PrincipalID != filter.ActorPrincipalID {
		return false
	}
	if filter.ResourceKind != "" && event.Resource.Kind != filter.ResourceKind {
		return false
	}
	if filter.ResourceID != "" && event.Resource.ID != filter.ResourceID {
		return false
	}
	if filter.CorrelationID != "" && event.CorrelationID != filter.CorrelationID {
		return false
	}
	return true
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if strings.TrimSpace(candidate) == value {
			return true
		}
	}
	return false
}

func idempotencyIndexKey(event model.Event) string {
	if event.IdempotencyKey == "" {
		return ""
	}
	source := firstNonEmpty(event.Source.Service, event.Source.Component, event.Source.NodeID, event.Source.PodName, event.Source.NodeName)
	if source == "" {
		return ""
	}
	return source + "\x00" + event.IdempotencyKey
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func encodePageToken(t time.Time) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano)))
}

func decodePageToken(token string) (time.Time, error) {
	if strings.TrimSpace(token) == "" {
		return time.Time{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid page token", model.ErrInvalidEvent)
	}
	parsed, err := time.Parse(time.RFC3339Nano, string(raw))
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid page token", model.ErrInvalidEvent)
	}
	return parsed, nil
}
