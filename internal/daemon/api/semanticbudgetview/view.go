package semanticbudgetview

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

const (
	DefaultPageSize  = 100
	MaxPageSize      = 500
	cursorVersion    = 1
	defaultCursorTTL = 15 * time.Minute
)

var (
	ErrInvalidCursor = errors.New("invalid semantic provider budget cursor")
	ErrRetentionRace = errors.New("semantic provider budget retention changed during pagination")
)

type Codec struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

type cursor struct {
	Version     int    `json:"version"`
	Kind        string `json:"kind"`
	SpaceID     string `json:"space_id"`
	BindingHash string `json:"binding_hash"`
	HighWater   uint64 `json:"high_water"`
	Position    uint64 `json:"position,omitempty"`
	Start       uint64 `json:"start,omitempty"`
	Offset      int    `json:"offset,omitempty"`
	SetDigest   string `json:"set_digest,omitempty"`
	SetCount    int    `json:"set_count,omitempty"`
	ExpiresAt   int64  `json:"expires_at"`
}

type ScopeFilter struct {
	SpaceID             string
	CorrelationID       string
	SemanticRuleID      string
	EmbeddingBindingKey string
}

type ScopePage struct {
	Scopes        []domaininference.BudgetScope
	NextPageToken string
}

type EventPage struct {
	Events                   []domaininference.BudgetAttemptEvent
	NextPageToken            string
	EarliestRetainedSequence uint64
	SnapshotHighWater        uint64
	CurrentHighWater         uint64
	HistoryGap               bool
}

func New(key []byte) (*Codec, error) {
	return NewWithClock(key, defaultCursorTTL, time.Now)
}

func NewWithClock(key []byte, ttl time.Duration, now func() time.Time) (*Codec, error) {
	if len(key) < 16 {
		return nil, fmt.Errorf("semantic budget cursor key must contain at least 16 bytes")
	}
	if ttl <= 0 || now == nil {
		return nil, fmt.Errorf("semantic budget cursor ttl and clock are required")
	}
	return &Codec{key: append([]byte(nil), key...), ttl: ttl, now: now}, nil
}

func NormalizePageSize(size int32) (int, error) {
	if size < 0 || size > MaxPageSize {
		return 0, fmt.Errorf("page_size must be between 0 and %d", MaxPageSize)
	}
	if size == 0 {
		return DefaultPageSize, nil
	}
	return int(size), nil
}

func (c *Codec) ListScopes(scopes []domaininference.BudgetScope, filter ScopeFilter, pageSize int, token, principal string) (ScopePage, error) {
	binding := hashBinding("scopes", principal, filter.SpaceID, filter.CorrelationID, filter.SemanticRuleID, filter.EmbeddingBindingKey)
	filtered := filterScopes(scopes, filter)
	highWater := uint64(0)
	for _, scope := range filtered {
		if scope.ScopeSequence > highWater {
			highWater = scope.ScopeSequence
		}
	}
	offset := 0
	if strings.TrimSpace(token) != "" {
		cur, err := c.decode(token)
		if err != nil || cur.Kind != "scopes" || cur.SpaceID != filter.SpaceID || cur.BindingHash != binding || cur.Offset < 0 {
			return ScopePage{}, ErrInvalidCursor
		}
		highWater = cur.HighWater
		offset = cur.Offset
		filtered = filterScopesThrough(scopes, filter, highWater)
		digest := scopeSetDigest(filtered)
		if len(filtered) != cur.SetCount || !hmac.Equal([]byte(digest), []byte(cur.SetDigest)) {
			return ScopePage{}, ErrRetentionRace
		}
	}
	filtered = scopesThrough(filtered, highWater)
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ScopeSequence < filtered[j].ScopeSequence })
	if offset > len(filtered) {
		return ScopePage{}, ErrInvalidCursor
	}
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	page := ScopePage{Scopes: append([]domaininference.BudgetScope(nil), filtered[offset:end]...)}
	if end < len(filtered) {
		var err error
		page.NextPageToken, err = c.encode(cursor{Version: cursorVersion, Kind: "scopes", SpaceID: filter.SpaceID, BindingHash: binding, HighWater: highWater, Offset: end, SetDigest: scopeSetDigest(filtered), SetCount: len(filtered)})
		if err != nil {
			return ScopePage{}, err
		}
	}
	return page, nil
}

func (c *Codec) ValidateEventContinuation(token, spaceID, scopeID, principal string) error {
	if strings.TrimSpace(token) == "" {
		return ErrInvalidCursor
	}
	cur, err := c.decode(token)
	binding := hashBinding("events", principal, spaceID, scopeID)
	if err != nil || cur.Kind != "events" || cur.SpaceID != spaceID || cur.BindingHash != binding {
		return ErrInvalidCursor
	}
	return nil
}

func (c *Codec) ListEvents(scope domaininference.BudgetScope, after uint64, pageSize int, token, principal string) (EventPage, error) {
	binding := hashBinding("events", principal, scope.SpaceID, string(scope.ID))
	current := eventHighWater(scope.AttemptEvents)
	snapshot := current
	start := after
	position := after
	if strings.TrimSpace(token) != "" {
		cur, err := c.decode(token)
		if err != nil || cur.Kind != "events" || cur.SpaceID != scope.SpaceID || cur.BindingHash != binding {
			return EventPage{}, ErrInvalidCursor
		}
		snapshot = cur.HighWater
		start = cur.Start
		position = cur.Position
	}
	earliest := uint64(0)
	if len(scope.AttemptEvents) > 0 {
		earliest = scope.AttemptEvents[0].Sequence
	}
	page := EventPage{EarliestRetainedSequence: earliest, SnapshotHighWater: snapshot, CurrentHighWater: current}
	if earliest > 0 && start != ^uint64(0) && start+1 < earliest {
		if strings.TrimSpace(token) != "" {
			return EventPage{}, ErrRetentionRace
		}
		page.HistoryGap = true
		return page, nil
	}
	if earliest == 0 && current > 0 && start < snapshot {
		if strings.TrimSpace(token) != "" {
			return EventPage{}, ErrRetentionRace
		}
		page.HistoryGap = true
		return page, nil
	}
	for _, event := range scope.AttemptEvents {
		if event.Sequence <= position || event.Sequence > snapshot {
			continue
		}
		page.Events = append(page.Events, event)
		position = event.Sequence
		if len(page.Events) == pageSize {
			break
		}
	}
	if position < snapshot {
		var err error
		page.NextPageToken, err = c.encode(cursor{Version: cursorVersion, Kind: "events", SpaceID: scope.SpaceID, BindingHash: binding, HighWater: snapshot, Position: position, Start: start})
		if err != nil {
			return EventPage{}, err
		}
	}
	return page, nil
}

func filterScopes(scopes []domaininference.BudgetScope, filter ScopeFilter) []domaininference.BudgetScope {
	return filterScopesThrough(scopes, filter, ^uint64(0))
}

func filterScopesThrough(scopes []domaininference.BudgetScope, filter ScopeFilter, highWater uint64) []domaininference.BudgetScope {
	out := make([]domaininference.BudgetScope, 0, len(scopes))
	for _, scope := range scopes {
		if scope.ScopeSequence > highWater || scope.SpaceID != filter.SpaceID || filter.CorrelationID != "" && scope.CorrelationID != filter.CorrelationID || filter.SemanticRuleID != "" && scope.SemanticRuleID != filter.SemanticRuleID || filter.EmbeddingBindingKey != "" && scope.EmbeddingBindingKey != filter.EmbeddingBindingKey {
			continue
		}
		out = append(out, scope)
	}
	return out
}

func scopesThrough(scopes []domaininference.BudgetScope, highWater uint64) []domaininference.BudgetScope {
	out := scopes[:0]
	for _, scope := range scopes {
		if scope.ScopeSequence <= highWater {
			out = append(out, scope)
		}
	}
	return out
}

func scopeSetDigest(scopes []domaininference.BudgetScope) string {
	ids := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		ids = append(ids, fmt.Sprintf("%020d:%s", scope.ScopeSequence, scope.ID))
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	return hex.EncodeToString(sum[:])
}

func eventHighWater(events []domaininference.BudgetAttemptEvent) uint64 {
	var high uint64
	for _, event := range events {
		if event.Sequence > high {
			high = event.Sequence
		}
	}
	return high
}

func hashBinding(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func (c *Codec) encode(value cursor) (string, error) {
	value.ExpiresAt = c.now().UTC().Add(c.ttl).UnixNano()
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(raw)
	signed := append(raw, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(signed), nil
}

func (c *Codec) decode(value string) (cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil || len(raw) <= sha256.Size {
		return cursor{}, ErrInvalidCursor
	}
	body, signature := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return cursor{}, ErrInvalidCursor
	}
	var decoded cursor
	if err := json.Unmarshal(body, &decoded); err != nil || decoded.Version != cursorVersion || decoded.ExpiresAt <= 0 || !c.now().UTC().Before(time.Unix(0, decoded.ExpiresAt)) {
		return cursor{}, ErrInvalidCursor
	}
	return decoded, nil
}
