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

func TestSystemMetadataIsSeparateFromModelOutput(t *testing.T) {
	res, err := Parse("json", nil, `{"input_hash":"model-forgery"}`)
	if err != nil {
		t.Fatal(err)
	}
	res.System = map[string]any{"input_hash": "daemon-hash", "procedure_version": 3}
	got, err := Resolve(res, "$system.input_hash", nil)
	if err != nil || got != "daemon-hash" {
		t.Fatalf("system input hash = %#v, err=%v", got, err)
	}
	got, err = Resolve(res, "$result.input_hash", nil)
	if err != nil || got != "model-forgery" {
		t.Fatalf("model input hash = %#v, err=%v", got, err)
	}
}

func TestParseJSONRejectsMissingRequired(t *testing.T) {
	_, err := Parse("json", json.RawMessage(`{"type":"object","required":["summary"]}`), `{}`)
	if err == nil {
		t.Fatal("expected error")
	}
}
