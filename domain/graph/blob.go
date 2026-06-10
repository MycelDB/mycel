package graph

import (
	"encoding/hex"
	"fmt"
	"time"
)

// BlobID is the content address of a stored blob: the lowercase hex-encoded
// SHA-256 digest of the blob's bytes. Identical content always yields the
// same BlobID, which gives deduplication and integrity verification for free.
type BlobID string

// blobIDRawLen is the raw digest length in bytes (SHA-256).
const blobIDRawLen = 32

// Bytes returns the raw 32-byte digest for a BlobID.
func (id BlobID) Bytes() ([]byte, error) {
	raw, err := hex.DecodeString(string(id))
	if err != nil {
		return nil, fmt.Errorf("invalid blob id %q: %w", id, err)
	}
	if len(raw) != blobIDRawLen {
		return nil, fmt.Errorf("invalid blob id %q: expected %d bytes, got %d", id, blobIDRawLen, len(raw))
	}
	return raw, nil
}

// BlobIDFromBytes builds a BlobID from a raw 32-byte SHA-256 digest.
func BlobIDFromBytes(raw []byte) (BlobID, error) {
	if len(raw) != blobIDRawLen {
		return "", fmt.Errorf("invalid blob digest: expected %d bytes, got %d", blobIDRawLen, len(raw))
	}
	return BlobID(hex.EncodeToString(raw)), nil
}

// BlobMeta describes a stored blob attached to a node.
type BlobMeta struct {
	ID               BlobID
	SizeBytes        int64
	MimeType         string // sniffed from content; authoritative
	DeclaredMimeType string // as provided by the caller, may be empty
	OriginalFilename string // as provided by the caller, may be empty
	CreatedAt        time.Time
}
