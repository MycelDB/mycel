package output

import (
	"encoding/json"
	"testing"
)

func TestParseJSONValidatesRequired(t *testing.T) {
	res, err := Parse("json", json.RawMessage(`{"type":"object","required":["summary"]}`), `{"summary":"ok","topics":["a"]}`)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Resolve(res, "$result.summary", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v != "ok" {
		t.Fatalf("summary = %v", v)
	}
	items, err := Items(res, "$result.topics")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0] != "a" {
		t.Fatalf("items = %+v", items)
	}
}

func TestParseJSONRejectsMissingRequired(t *testing.T) {
	_, err := Parse("json", json.RawMessage(`{"type":"object","required":["summary"]}`), `{}`)
	if err == nil {
		t.Fatal("expected error")
	}
}
