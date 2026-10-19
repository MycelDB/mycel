package engine

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
)

const (
	indexedCursorPrefix      = "qrx7:"
	legacyShapedCursorPrefix = "shapeidx:"
	indexedCursorVersion     = 1
	cursorModeSource         = "source"
	cursorModeOutput         = "output"
)

type indexedCursor struct {
	Version          int    `json:"version"`
	PlanHash         string `json:"plan_hash"`
	TransactionID    string `json:"transaction_id"`
	BaseRevision     int64  `json:"base_revision"`
	SchemaVersion    string `json:"schema_version,omitempty"`
	ReadViewIdentity string `json:"read_view_identity,omitempty"`
	Mode             string `json:"mode"`
	StartIndex       int    `json:"start_index,omitempty"`
	PageCursor       string `json:"page_cursor,omitempty"`
	PageOffset       int    `json:"page_offset,omitempty"`
	OutputOffset     int    `json:"output_offset,omitempty"`
	RowsReturned     int64  `json:"rows_returned,omitempty"`
	SemanticIdentity string `json:"semantic_identity,omitempty"`
	Checksum         string `json:"checksum"`
}

type legacyShapedCursor struct {
	IndexCursor  string `json:"index_cursor,omitempty"`
	RowsReturned int64  `json:"rows_returned,omitempty"`
}

type legacyAdjacencyCursor struct {
	StartIndex   int    `json:"start_index"`
	EdgeCursor   string `json:"edge_cursor,omitempty"`
	RowsReturned int64  `json:"rows_returned,omitempty"`
}

func decodeIndexedCursor(prepared PreparedQuery) (indexedCursor, error) {
	token := strings.TrimSpace(prepared.Statement.Read.Page.Cursor)
	if token == "" {
		return indexedCursor{Mode: cursorModeSource}, nil
	}
	if strings.HasPrefix(token, indexedCursorPrefix) {
		payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, indexedCursorPrefix))
		if err != nil {
			return indexedCursor{}, staleCursor("cursor payload is malformed", err)
		}
		var cursor indexedCursor
		if err := json.Unmarshal(payload, &cursor); err != nil {
			return indexedCursor{}, staleCursor("cursor payload is malformed", err)
		}
		if err := validateIndexedCursor(prepared, cursor); err != nil {
			return indexedCursor{}, err
		}
		return cursor, nil
	}
	if prepared.readViewIdentity != "" && prepared.readViewIdentity != "0" {
		return indexedCursor{}, staleCursor("legacy cursor is not bound to the current transaction overlay", nil)
	}
	if strings.HasPrefix(token, legacyShapedCursorPrefix) {
		if !legacyIndexSourceFamily(prepared.Physical.Family) || !nativeSourceCursorSupported(prepared) {
			return indexedCursor{}, staleCursor("legacy shaped cursor is not compatible with the indexed plan", nil)
		}
		payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, legacyShapedCursorPrefix))
		if err != nil {
			return indexedCursor{}, staleCursor("legacy index cursor is malformed", err)
		}
		var legacy legacyShapedCursor
		if err := json.Unmarshal(payload, &legacy); err != nil || legacy.RowsReturned < 0 {
			return indexedCursor{}, staleCursor("legacy index cursor is malformed", err)
		}
		return indexedCursor{Mode: cursorModeSource, PageCursor: legacy.IndexCursor, RowsReturned: legacy.RowsReturned}, nil
	}
	if offset, err := strconv.Atoi(token); err == nil && offset >= 0 {
		return indexedCursor{Mode: cursorModeOutput, OutputOffset: offset, RowsReturned: int64(offset)}, nil
	}
	if prepared.Physical.Family == queryphysical.FamilyIndexedAdjacency && nativeSourceCursorSupported(prepared) {
		if strings.HasPrefix(token, "multi:") {
			payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "multi:"))
			if err != nil {
				return indexedCursor{}, staleCursor("legacy adjacency cursor is malformed", err)
			}
			var legacy legacyAdjacencyCursor
			if err := json.Unmarshal(payload, &legacy); err != nil || legacy.StartIndex < 0 || legacy.RowsReturned < 0 {
				return indexedCursor{}, staleCursor("legacy adjacency cursor is malformed", err)
			}
			return indexedCursor{Mode: cursorModeSource, StartIndex: legacy.StartIndex, PageCursor: legacy.EdgeCursor, RowsReturned: legacy.RowsReturned}, nil
		}
		return indexedCursor{Mode: cursorModeSource, PageCursor: token}, nil
	}
	if legacyIndexSourceFamily(prepared.Physical.Family) && nativeSourceCursorSupported(prepared) {
		return indexedCursor{Mode: cursorModeSource, PageCursor: token}, nil
	}
	return indexedCursor{}, staleCursor("cursor is not valid for the indexed plan", nil)
}

func legacyIndexSourceFamily(family queryphysical.Family) bool {
	switch family {
	case queryphysical.FamilyIndexedOrder, queryphysical.FamilyIndexedEquality, queryphysical.FamilyIndexedSubtree:
		return true
	default:
		return false
	}
}

func validateIndexedCursor(prepared PreparedQuery, cursor indexedCursor) error {
	if cursor.Version != indexedCursorVersion {
		return staleCursor("cursor version is unsupported", nil)
	}
	wantChecksum := indexedCursorChecksum(cursor)
	if subtle.ConstantTimeCompare([]byte(cursor.Checksum), []byte(wantChecksum)) != 1 {
		return staleCursor("cursor integrity checksum does not match", nil)
	}
	if cursor.Mode != cursorModeSource && cursor.Mode != cursorModeOutput {
		return staleCursor("cursor mode is unsupported", nil)
	}
	if cursor.StartIndex < 0 || cursor.PageOffset < 0 || cursor.OutputOffset < 0 || cursor.RowsReturned < 0 {
		return staleCursor("cursor contains a negative position", nil)
	}
	if cursor.Mode == cursorModeSource && cursor.OutputOffset != 0 {
		return staleCursor("source cursor contains output continuation state", nil)
	}
	if cursor.Mode == cursorModeSource && !nativeSourceCursorSupported(prepared) {
		return staleCursor("source cursor is not supported by the indexed plan", nil)
	}
	if prepared.Statement.Read != nil && prepared.Physical.Family != queryphysical.FamilyIndexedSubtree && prepared.Statement.Read.Page.Limit > 0 && cursor.RowsReturned >= prepared.Statement.Read.Page.Limit {
		return staleCursor("cursor returned-row count reaches or exceeds the query limit", nil)
	}
	if cursor.Mode == cursorModeSource && prepared.Physical.Family != queryphysical.FamilyIndexedAdjacency && cursor.StartIndex != 0 {
		return staleCursor("source cursor contains incompatible traversal state", nil)
	}
	if cursor.Mode == cursorModeOutput && (cursor.StartIndex != 0 || cursor.PageCursor != "" || cursor.PageOffset != 0 || cursor.RowsReturned != int64(cursor.OutputOffset)) {
		return staleCursor("output cursor contains incompatible source continuation state", nil)
	}
	if prepared.Physical.Family != queryphysical.FamilySemantic && cursor.SemanticIdentity != "" {
		return staleCursor("cursor contains incompatible semantic continuation state", nil)
	}
	if cursor.TransactionID != prepared.Planning.Transaction.ID || cursor.BaseRevision != prepared.Planning.Transaction.BaseRevision {
		return staleCursor("cursor transaction snapshot does not match the prepared query", nil)
	}
	// Version-1 cursors issued before read-view binding omitted this field. They
	// remain compatible only with an unchanged/empty overlay.
	if cursor.ReadViewIdentity != prepared.readViewIdentity && !(cursor.ReadViewIdentity == "" && prepared.readViewIdentity == "0") {
		return staleCursor("cursor transaction overlay does not match the current read view", nil)
	}
	if cursor.SchemaVersion != prepared.Physical.SchemaVersion {
		return staleCursor("cursor schema version does not match the prepared query", nil)
	}
	if cursor.PlanHash != indexedPlanHash(prepared) {
		return staleCursor("cursor does not match the prepared query plan", nil)
	}
	return nil
}

func encodeIndexedCursor(prepared PreparedQuery, cursor indexedCursor) string {
	cursor.Version = indexedCursorVersion
	cursor.PlanHash = indexedPlanHash(prepared)
	cursor.TransactionID = prepared.Planning.Transaction.ID
	cursor.BaseRevision = prepared.Planning.Transaction.BaseRevision
	cursor.SchemaVersion = prepared.Physical.SchemaVersion
	cursor.ReadViewIdentity = prepared.readViewIdentity
	cursor.Checksum = indexedCursorChecksum(cursor)
	payload, _ := json.Marshal(cursor)
	return indexedCursorPrefix + base64.RawURLEncoding.EncodeToString(payload)
}

func indexedCursorChecksum(cursor indexedCursor) string {
	cursor.Checksum = ""
	payload, _ := json.Marshal(cursor)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func semanticCandidateIdentity(ids []string) string {
	payload, _ := json.Marshal(ids)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func indexedPlanHash(prepared PreparedQuery) string {
	statement := prepared.Statement.Comparable()
	payload, _ := json.Marshal(struct {
		Statement  querylogical.Statement          `json:"statement"`
		Parameters map[string]querylogical.Value   `json:"parameters,omitempty"`
		Family     queryphysical.Family            `json:"family"`
		Access     queryphysical.AccessPath        `json:"access"`
		Ordering   queryphysical.OrderingGuarantee `json:"ordering"`
	}{
		Statement: statement, Parameters: prepared.Planning.Parameters,
		Family: prepared.Physical.Family, Access: prepared.Physical.Access, Ordering: prepared.Physical.Ordering,
	})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func staleCursor(message string, cause error) error {
	if cause != nil {
		message = fmt.Sprintf("%s: %v", message, cause)
	}
	return querylogical.NewError(querylogical.ErrorStaleCursor, "decode indexed cursor", message, cause)
}
