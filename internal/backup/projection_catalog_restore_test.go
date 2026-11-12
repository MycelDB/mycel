package backup

import (
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/projection"
)

func validProjectionArtifactCatalog() projection.BackupCatalog {
	return projection.BackupCatalog{
		Version:        projection.BackupCatalogVersion,
		PartitionCount: 1,
		Partitions: []projection.BackupPartitionCatalog{{
			PartitionID:  0,
			SnapshotHash: "sha256:" + strings.Repeat("0", 64),
			Projections: []projection.BackupProjectionCatalog{{
				ProjectionID:        "prj_restore",
				Kind:                projection.KindMetadata,
				Scope:               projection.Scope{SpaceID: "space", DomainID: "domain"},
				ConfigurationHash:   "sha256:configuration",
				MaterializerVersion: "materializer-v1",
				ConfigurationEpoch:  3,
				PublishedRevision:   9,
				FencingEpoch:        4,
				ArtifactGeneration:  "generation-9",
			}},
			Artifacts: []projection.ArtifactManifest{{
				ProjectionID:        "prj_restore",
				Kind:                projection.KindMetadata,
				Scope:               projection.Scope{SpaceID: "space", DomainID: "domain"},
				ConfigurationHash:   "sha256:configuration",
				ConfigurationEpoch:  3,
				MaterializerVersion: "materializer-v1",
				Source:              projection.Frontier{Revision: 9},
				FencingEpoch:        4,
				Generation:          "generation-9",
				Format:              "artifact-v1",
				Checksum:            "sha256:artifact",
			}},
		}},
	}
}

func TestProjectionArtifactRestorePreflightRejectsMismatches(t *testing.T) {
	missing := validProjectionArtifactCatalog()
	missing.Partitions[0].Artifacts = nil
	if err := missing.Validate(); err != nil {
		t.Fatalf("missing artifact should remain rebuildable: %v", err)
	}

	tests := []struct {
		name string
		edit func(*projection.BackupCatalog)
	}{
		{name: "partial", edit: func(c *projection.BackupCatalog) { c.Partitions[0].Artifacts[0].Format = "" }},
		{name: "stale", edit: func(c *projection.BackupCatalog) { c.Partitions[0].Artifacts[0].Generation = "generation-8" }},
		{name: "newer_source", edit: func(c *projection.BackupCatalog) { c.Partitions[0].Artifacts[0].Source.Revision = 10 }},
		{name: "configuration", edit: func(c *projection.BackupCatalog) { c.Partitions[0].Artifacts[0].ConfigurationEpoch = 4 }},
		{name: "fencing", edit: func(c *projection.BackupCatalog) { c.Partitions[0].Artifacts[0].FencingEpoch = 5 }},
		{name: "corrupt_checksum", edit: func(c *projection.BackupCatalog) { c.Partitions[0].Artifacts[0].Checksum = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := validProjectionArtifactCatalog()
			test.edit(&catalog)
			if err := catalog.Validate(); err == nil {
				t.Fatal("mismatched artifact accepted")
			}
		})
	}
}
