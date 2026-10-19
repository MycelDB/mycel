package engine

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func TestDecodeIndexedCursorRejectsChecksummedIncompatibleModeState(t *testing.T) {
	query := querylogical.Query{
		Start: querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
		Page:  querylogical.PageSpec{Limit: 2},
	}
	prepared := PreparedQuery{
		Statement: querylogical.ReadStatement(query),
		Physical:  queryphysical.Plan{Family: queryphysical.FamilyLabelIndex, SchemaVersion: "schema-1"},
		Planning:  PlanningContext{Transaction: sessionmodel.GraphTransaction{ID: "tx-1", BaseRevision: 7}},
	}

	for _, test := range []struct {
		name   string
		cursor indexedCursor
	}{
		{name: "output with source cursor", cursor: indexedCursor{Mode: cursorModeOutput, PageCursor: "forged", OutputOffset: 2, RowsReturned: 2}},
		{name: "output with source offset", cursor: indexedCursor{Mode: cursorModeOutput, PageOffset: 1, OutputOffset: 2, RowsReturned: 2}},
		{name: "output row count mismatch", cursor: indexedCursor{Mode: cursorModeOutput, OutputOffset: 2, RowsReturned: 3}},
		{name: "source with output offset", cursor: indexedCursor{Mode: cursorModeSource, OutputOffset: 2, RowsReturned: 2}},
		{name: "source row count beyond query limit", cursor: indexedCursor{Mode: cursorModeSource, RowsReturned: 3}},
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

func TestDecodeIndexedCursorRejectsLegacySourceCursorForBlockingShape(t *testing.T) {
	for _, token := range []string{
		"shapeidx:eyJpbmRleF9jdXJzb3IiOiJvcGFxdWUiLCJyb3dzX3JldHVybmVkIjoxfQ",
		"opaque-index-key",
	} {
		t.Run(strings.TrimPrefix(token, legacyShapedCursorPrefix), func(t *testing.T) {
			query := querylogical.Query{
				Start:    querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
				Distinct: true,
				Page:     querylogical.PageSpec{Cursor: token},
			}
			prepared := PreparedQuery{
				Statement: querylogical.ReadStatement(query),
				Physical:  queryphysical.Plan{Family: queryphysical.FamilyIndexedEquality},
			}
			_, err := decodeIndexedCursor(prepared)
			var queryErr *querylogical.QueryError
			if !errors.As(err, &queryErr) || queryErr.Code != querylogical.ErrorStaleCursor {
				t.Fatalf("error = %#v, want stale cursor", err)
			}
		})
	}
}

func TestDecodeIndexedCursorRejectsSourceModeForMaterializedTagPlan(t *testing.T) {
	query := querylogical.Query{Start: querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}}}
	prepared := PreparedQuery{
		Statement: querylogical.ReadStatement(query),
		Physical: queryphysical.Plan{
			Family: queryphysical.FamilyTagIndex,
			Access: queryphysical.AccessPath{TagBranches: [][]string{{"featured"}}},
		},
		Planning: PlanningContext{Transaction: sessionmodel.GraphTransaction{ID: "tx-1", BaseRevision: 7}},
	}
	token := encodeIndexedCursor(prepared, indexedCursor{Mode: cursorModeSource, PageCursor: "forged", RowsReturned: 1})
	query.Page.Cursor = token
	prepared.Statement = querylogical.ReadStatement(query)
	_, err := decodeIndexedCursor(prepared)
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != querylogical.ErrorStaleCursor {
		t.Fatalf("error = %#v, want stale cursor", err)
	}
}

func TestIndexedSourceRejectsChecksumValidOffsetOutsideSourceBatch(t *testing.T) {
	query := querylogical.Query{
		Start: querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
		Page:  querylogical.PageSpec{Size: 1},
	}
	prepared := PreparedQuery{
		Statement: querylogical.ReadStatement(query),
		Physical: queryphysical.Plan{
			Family: queryphysical.FamilyLabelIndex,
			Access: queryphysical.AccessPath{Kind: queryphysical.AccessLabel, StartAlias: "n", Labels: []string{"Document"}},
		},
	}
	tracker, err := queryexecution.NewTracker(queryexecution.ResolveBudget(queryexecution.Budget{BatchSize: 2}, queryexecution.Budget{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = (&Engine{}).indexedSource(t.Context(), ExecutionContext{}, prepared, tracker, indexedCursor{Mode: cursorModeSource, PageOffset: 2}, 0)
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != querylogical.ErrorStaleCursor {
		t.Fatalf("error = %#v, want stale cursor", err)
	}
}
