package engine

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func TestDecodeIndexedCursorRejectsChecksummedIncompatibleModeState(t *testing.T) {
	query := querylogical.Query{Start: querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}}}
	prepared := PreparedQuery{
		Statement: querylogical.ReadStatement(query),
		Physical:  queryphysical.Plan{Family: queryphysical.FamilyLabelIndex, SchemaVersion: "schema-1"},
		Planning:  PlanningContext{Transaction: sessionmodel.GraphTransaction{ID: "tx-1", BaseRevision: 7}},
		Kind:      PlanIndexed,
	}

	for _, test := range []struct {
		name   string
		cursor indexedCursor
	}{
		{name: "output with source cursor", cursor: indexedCursor{Mode: cursorModeOutput, PageCursor: "forged", OutputOffset: 2, RowsReturned: 2}},
		{name: "output with source offset", cursor: indexedCursor{Mode: cursorModeOutput, PageOffset: 1, OutputOffset: 2, RowsReturned: 2}},
		{name: "output row count mismatch", cursor: indexedCursor{Mode: cursorModeOutput, OutputOffset: 2, RowsReturned: 3}},
		{name: "source with output offset", cursor: indexedCursor{Mode: cursorModeSource, OutputOffset: 2, RowsReturned: 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			token := encodeIndexedCursor(prepared, test.cursor)
			payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, indexedCursorPrefix))
			if err != nil {
				t.Fatal(err)
			}
			var cursor indexedCursor
			if err := json.Unmarshal(payload, &cursor); err != nil {
				t.Fatal(err)
			}
			cursor.Checksum = indexedCursorChecksum(cursor)
			payload, err = json.Marshal(cursor)
			if err != nil {
				t.Fatal(err)
			}
			withCursor := query
			withCursor.Page.Cursor = indexedCursorPrefix + base64.RawURLEncoding.EncodeToString(payload)
			prepared.Statement = querylogical.ReadStatement(withCursor)
			_, err = decodeIndexedCursor(prepared)
			var queryErr *querylogical.QueryError
			if !errors.As(err, &queryErr) || queryErr.Code != querylogical.ErrorStaleCursor {
				t.Fatalf("error = %#v, want stale cursor", err)
			}
		})
	}
}
