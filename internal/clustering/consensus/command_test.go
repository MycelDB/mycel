package consensus

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

func TestNewSpaceCommandValidatesPartition(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.MustParse("00000000-0000-0000-0000-000000000001"))
	cmd, err := NewSpaceCommand(spaceID, 64, wal.RecordType("space.create"), []byte(`{"name":"main"}`), "cmd-1")
	if err != nil {
		t.Fatalf("NewSpaceCommand() error = %v", err)
	}
	if err := cmd.Validate(64); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cmd.Scope != CommandScopeSpacePartition || cmd.SpaceID != spaceID.String() || cmd.CommandID != "cmd-1" {
		t.Fatalf("unexpected command: %+v", cmd)
	}
	if len(cmd.CommandHash) == 0 {
		t.Fatal("expected command hash")
	}
}

func TestCommandEncodeDecode(t *testing.T) {
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.user.create"), []byte(`{"user":"a"}`), "cmd-1")
	encoded, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}
	decoded, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("DecodeCommand() error = %v", err)
	}
	if err := decoded.Validate(64); err != nil {
		t.Fatalf("decoded Validate() error = %v", err)
	}
	if decoded.Version != cmd.Version || decoded.Scope != cmd.Scope || decoded.RecordType != cmd.RecordType || decoded.CommandID != cmd.CommandID || !bytes.Equal(decoded.Payload, cmd.Payload) || !bytes.Equal(decoded.CommandHash, cmd.CommandHash) {
		t.Fatalf("decoded command mismatch: got=%+v want=%+v", decoded, cmd)
	}
}

func TestCommandValidateRejectsInvalid(t *testing.T) {
	valid := NewCommand(CommandScopeSystem, wal.RecordType("system.op"), []byte(`{}`), "cmd-1")
	cases := []struct {
		name string
		cmd  RaftCommand
	}{
		{"bad version", func() RaftCommand { c := valid; c.Version = 99; return c }()},
		{"bad scope", func() RaftCommand { c := valid; c.Scope = "bad"; return c }()},
		{"system with partition", func() RaftCommand { c := valid; c.PartitionID = 1; return c }()},
		{"missing record", func() RaftCommand { c := valid; c.RecordType = ""; c.CommandHash = c.ComputeHash(); return c }()},
		{"missing command id", func() RaftCommand { c := valid; c.CommandID = ""; return c }()},
		{"hash mismatch", func() RaftCommand { c := valid; c.Payload = []byte(`{"changed":true}`); return c }()},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cmd.Validate(64); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestSpaceCommandRejectsMismatchedPartition(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.MustParse("00000000-0000-0000-0000-000000000001"))
	cmd, err := NewSpaceCommand(spaceID, 64, wal.RecordType("space.create"), []byte(`{}`), "cmd-1")
	if err != nil {
		t.Fatalf("NewSpaceCommand() error = %v", err)
	}
	cmd.PartitionID++
	cmd.CommandHash = cmd.ComputeHash()
	if err := cmd.Validate(64); err == nil {
		t.Fatal("expected mismatched partition to fail")
	}
}
