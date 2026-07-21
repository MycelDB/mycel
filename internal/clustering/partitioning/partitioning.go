package partitioning

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/google/uuid"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const DefaultPartitionCount uint32 = 64

type PartitionID uint32

type Config struct {
	PartitionCount uint32
}

func (c Config) Validate() error {
	if c.PartitionCount == 0 {
		return fmt.Errorf("partition count must be positive")
	}
	return nil
}

func PartitionForSpace(spaceID string, partitionCount uint32) (PartitionID, error) {
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return 0, fmt.Errorf("space_id is required")
	}
	parsed, err := uuid.Parse(spaceID)
	if err != nil {
		return 0, fmt.Errorf("space_id must be a UUID")
	}
	return PartitionForSpaceID(domainspace.SpaceID(parsed), partitionCount)
}

func PartitionForSpaceID(spaceID domainspace.SpaceID, partitionCount uint32) (PartitionID, error) {
	if spaceID == uuid.Nil {
		return 0, fmt.Errorf("space_id is required")
	}
	if partitionCount == 0 {
		return 0, fmt.Errorf("partition count must be positive")
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(spaceID.String()))
	return PartitionID(h.Sum64() % uint64(partitionCount)), nil
}

func (p PartitionID) Uint32() uint32 { return uint32(p) }
