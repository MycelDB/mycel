package engine

import (
	"crypto/sha256"
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
	Version       int    `json:"version"`
	PlanHash      string `json:"plan_hash"`
	TransactionID string `json:"transaction_id"`
	BaseRevision  int64  `json:"base_revision"`
	SchemaVersion string `json:"schema_version,omitempty"`
	Mode          string `json:"mode"`
	PageCursor    string `json:"page_cursor,omitempty"`
	PageOffset    int    `json:"page_offset,omitempty"`
	OutputOffset  int    `json:"output_offset,omitempty"`
	RowsReturned  int64  `json:"rows_returned,omitempty"`
}

type legacyShapedCursor struct {
	IndexCursor  string `json:"index_cursor,omitempty"`
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
	if strings.HasPrefix(token, legacyShapedCursorPrefix) {
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
	if prepared.Physical.Family == queryphysical.FamilyIndexedOrder || prepared.Physical.Family == queryphysical.FamilyIndexedEquality {
		return indexedCursor{Mode: cursorModeSource, PageCursor: token}, nil
	}
	return indexedCursor{}, staleCursor("cursor is not valid for the indexed plan", nil)
}

func validateIndexedCursor(prepared PreparedQuery, cursor indexedCursor) error {
	if cursor.Version != indexedCursorVersion {
		return staleCursor("cursor version is unsupported", nil)
	}
	if cursor.Mode != cursorModeSource && cursor.Mode != cursorModeOutput {
		return staleCursor("cursor mode is unsupported", nil)
	}
	if cursor.PageOffset < 0 || cursor.OutputOffset < 0 || cursor.RowsReturned < 0 {
		return staleCursor("cursor contains a negative position", nil)
	}
	if cursor.TransactionID != prepared.Planning.Transaction.ID || cursor.BaseRevision != prepared.Planning.Transaction.BaseRevision {
		return staleCursor("cursor transaction snapshot does not match the prepared query", nil)
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
	payload, _ := json.Marshal(cursor)
	return indexedCursorPrefix + base64.RawURLEncoding.EncodeToString(payload)
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
