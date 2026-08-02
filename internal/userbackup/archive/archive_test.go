package archive

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriteReadRoundTrip(t *testing.T) {
	manifest := Manifest{SubjectUser: User{UserID: "user-1", Username: "alice"}, Spaces: []Space{{SourceSpaceID: "space-1", Name: "notes", Domains: []Domain{{SourceDomainID: "domain-1", Key: "default", Name: "Default", Default: true, DataPath: "domains/space-1/domain-1.json"}}}}}
	entries := []Entry{{Path: "domains/space-1/domain-1.json", Data: []byte(`{"format":"mycel-domain-json-v1"}`), ContentType: "application/json"}}
	var buf bytes.Buffer
	if err := Write(&buf, "zstd", manifest, entries); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	got, err := Read(bytes.NewReader(buf.Bytes()), "auto")
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if got.Manifest.Format != Format || got.Manifest.Version != 1 {
		t.Fatalf("unexpected manifest format/version: %s/%d", got.Manifest.Format, got.Manifest.Version)
	}
	if got.Manifest.SubjectUser.Username != "alice" {
		t.Fatalf("unexpected subject user: %q", got.Manifest.SubjectUser.Username)
	}
	if string(got.Entries["domains/space-1/domain-1.json"]) != string(entries[0].Data) {
		t.Fatalf("entry data mismatch")
	}
}

func TestValidateRejectsChecksumMismatch(t *testing.T) {
	manifest := Manifest{Format: Format, Version: 1, SubjectUser: User{UserID: "user-1"}, Spaces: []Space{{SourceSpaceID: "space-1", Name: "notes", Domains: []Domain{{SourceDomainID: "domain-1", Key: "default", Name: "Default", DataPath: "domains/space-1/domain-1.json"}}}}, Files: []File{{Path: "domains/space-1/domain-1.json", SizeBytes: 4, SHA256: strings.Repeat("0", 64)}}}
	if err := Validate(manifest, map[string][]byte{"domains/space-1/domain-1.json": []byte("data")}); err == nil {
		t.Fatalf("expected checksum validation error")
	}
}

func TestWriteRejectsUnsafePath(t *testing.T) {
	manifest := Manifest{SubjectUser: User{UserID: "user-1"}}
	var buf bytes.Buffer
	if err := Write(&buf, "none", manifest, []Entry{{Path: "../escape", Data: []byte("x")}}); err == nil {
		t.Fatalf("expected unsafe path error")
	}
}
