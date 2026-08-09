package consensus

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

type raftRecordCoverage struct {
	Subsystem string
	Scope     string
	Status    string
	Tranche   string
}

var phaseDRaftRecordCoverage = map[string]raftRecordCoverage{
	"identity.admin.put.v1":                {Subsystem: "admin identity", Scope: "system raft", Status: "covered", Tranche: "D0 verify"},
	"identity.admin.session.put.v1":        {Subsystem: "admin identity", Scope: "system raft", Status: "covered", Tranche: "D0 verify"},
	"identity.user.put.v1":                 {Subsystem: "user identity", Scope: "system raft", Status: "covered", Tranche: "D0 verify"},
	"identity.user.session.put.v1":         {Subsystem: "user identity", Scope: "system raft", Status: "covered", Tranche: "D0 verify"},
	"space.create_with_default_domain.v1":  {Subsystem: "space", Scope: "partition raft", Status: "covered", Tranche: "D1 verify"},
	"space.domain.create.v1":               {Subsystem: "space/domain", Scope: "partition raft", Status: "covered", Tranche: "D1 verify"},
	"space.domain.update.v1":               {Subsystem: "space/domain", Scope: "partition raft", Status: "covered", Tranche: "D1 verify"},
	"space.domain.delete.v1":               {Subsystem: "space/domain", Scope: "partition raft", Status: "covered", Tranche: "D1 verify"},
	"space.acl.grant.v1":                   {Subsystem: "space acl", Scope: "partition raft", Status: "covered", Tranche: "D1 verify"},
	"space.delete.v1":                      {Subsystem: "space", Scope: "partition raft", Status: "covered", Tranche: "D1 verify"},
	"graph.commit.v1":                      {Subsystem: "graph", Scope: "partition raft", Status: "covered", Tranche: "D0 verify"},
	"blob.meta.put.v1":                     {Subsystem: "blob", Scope: "partition raft", Status: "covered", Tranche: "D3 verify"},
	"blob.meta.delete.v1":                  {Subsystem: "blob", Scope: "partition raft", Status: "covered", Tranche: "D3 verify"},
	"schema.put.v1":                        {Subsystem: "schema", Scope: "partition raft", Status: "covered", Tranche: "D2 verify"},
	"schema.delete.v1":                     {Subsystem: "schema", Scope: "partition raft", Status: "covered", Tranche: "D2 verify"},
	"semantic.global.mutation.v1":          {Subsystem: "semantic", Scope: "system raft", Status: "covered", Tranche: "D4 verify"},
	"semantic.space.mutation.v1":           {Subsystem: "semantic", Scope: "partition raft", Status: "covered", Tranche: "D4 verify"},
	"semantic.maintenance.mutation.v1":     {Subsystem: "semantic maintenance", Scope: "partition raft", Status: "covered", Tranche: "D4 verify"},
	"semantic.accounting.mutation.v1":      {Subsystem: "semantic accounting", Scope: "system raft", Status: "covered", Tranche: "D4 verify"},
	"daemon.backup.policy.update.v1":       {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "D5 verify"},
	"daemon.backup.delete.v1":              {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "D5 verify"},
	"daemon.backup.cluster.request.v1":     {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "cluster backup plan phase 2"},
	"daemon.backup.cluster.phase.v1":       {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "cluster backup plan phase 2"},
	"daemon.backup.cluster.barrier.v1":     {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "cluster backup plan phase 2"},
	"daemon.backup.cluster.node_result.v1": {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "cluster backup plan phase 2"},
	"daemon.backup.cluster.complete.v1":    {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "cluster backup plan phase 2"},
	"daemon.backup.cluster.fail.v1":        {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "cluster backup plan phase 2"},
	"daemon.backup.cluster.abort.v1":       {Subsystem: "backup", Scope: "system raft", Status: "covered", Tranche: "cluster backup plan phase 2"},
}

func TestPhaseDRaftRecordCoverageClassifiesAllRecordTypes(t *testing.T) {
	records, err := discoverWALRecordTypes()
	if err != nil {
		t.Fatalf("discover WAL record types: %v", err)
	}
	for recordType, decl := range records {
		coverage, ok := phaseDRaftRecordCoverage[recordType]
		if !ok {
			t.Errorf("record type %q declared at %s is missing Phase D raft coverage classification", recordType, decl)
			continue
		}
		if strings.TrimSpace(coverage.Subsystem) == "" || strings.TrimSpace(coverage.Scope) == "" || strings.TrimSpace(coverage.Status) == "" || strings.TrimSpace(coverage.Tranche) == "" {
			t.Errorf("record type %q has incomplete Phase D coverage classification: %#v", recordType, coverage)
		}
	}
	for recordType := range phaseDRaftRecordCoverage {
		if _, ok := records[recordType]; !ok {
			t.Errorf("Phase D coverage classification contains stale record type %q", recordType)
		}
	}
}

func discoverWALRecordTypes() (map[string]string, error) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	internalDir := filepath.Join(repoRoot, "internal")
	re := regexp.MustCompile(`\b(recordType[A-Za-z0-9_]+)\s+wal\.RecordType\s*=\s*"([^"]+)"`)
	out := map[string]string{}
	err := filepath.WalkDir(internalDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range re.FindAllStringSubmatch(string(data), -1) {
			recordType := match[2]
			decl := strings.TrimPrefix(filepath.ToSlash(path), filepath.ToSlash(repoRoot)+"/") + ":" + match[1]
			out[recordType] = decl
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, os.ErrNotExist
	}
	return out, nil
}

func TestPhaseDRaftRecordCoverageSummary(t *testing.T) {
	covered, gaps := []string{}, []string{}
	for recordType, coverage := range phaseDRaftRecordCoverage {
		if coverage.Status == "covered" {
			covered = append(covered, recordType)
		} else {
			gaps = append(gaps, recordType)
		}
	}
	sort.Strings(covered)
	sort.Strings(gaps)
	if len(covered) == 0 {
		t.Fatalf("expected Phase D inventory to include covered records; covered=%v gaps=%v", covered, gaps)
	}
}
