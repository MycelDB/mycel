package graphstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const MaxIdempotencyRecordsPerStore = 100_000

type txnCommitMetadata struct {
	Idempotency *IdempotencyRecord `json:"idempotency,omitempty"`
}

func idempotencyRecordIdentity(scope, key string) string {
	return fmt.Sprintf("%d:%s%s", len(scope), scope, key)
}

func cloneIdempotencyRecord(record IdempotencyRecord) IdempotencyRecord {
	record.Metadata = append(json.RawMessage(nil), record.Metadata...)
	return record
}

func validateIdempotencyRecord(record IdempotencyRecord) error {
	if strings.TrimSpace(record.Scope) == "" {
		return fmt.Errorf("%w: idempotency scope is required", ErrInvalidRecord)
	}
	if strings.TrimSpace(record.Key) == "" {
		return fmt.Errorf("%w: idempotency key is required", ErrInvalidRecord)
	}
	if strings.TrimSpace(record.Digest) == "" {
		return fmt.Errorf("%w: idempotency digest is required", ErrInvalidRecord)
	}
	if record.CommittedAt.IsZero() {
		return fmt.Errorf("%w: idempotency committed_at is required", ErrInvalidRecord)
	}
	if !record.ExpiresAt.After(record.CommittedAt) {
		return fmt.Errorf("%w: idempotency expires_at must follow committed_at", ErrInvalidRecord)
	}
	return nil
}

// CheckIdempotencyRecord performs the same conflict, expiry, and capacity
// checks as CommitWithInfo without reserving or writing the candidate.
func (s *LocalStore) CheckIdempotencyRecord(ctx context.Context, candidate IdempotencyRecord) (IdempotencyRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return IdempotencyRecord{}, false, err
	}
	if err := validateIdempotencyRecord(candidate); err != nil {
		return IdempotencyRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return IdempotencyRecord{}, false, err
	}
	s.purgeExpiredIdempotencyRecordsLocked(candidate.CommittedAt)
	identity := idempotencyRecordIdentity(candidate.Scope, candidate.Key)
	if existing, ok := s.idempotencyRecords[identity]; ok {
		if existing.Digest != candidate.Digest {
			return IdempotencyRecord{}, false, ErrIdempotencyConflict
		}
		return cloneIdempotencyRecord(existing), true, nil
	}
	if len(s.idempotencyRecords) >= MaxIdempotencyRecordsPerStore {
		return IdempotencyRecord{}, false, ErrIdempotencyCapacity
	}
	return IdempotencyRecord{}, false, ctx.Err()
}

func (s *LocalStore) GetIdempotencyRecord(ctx context.Context, scope, key string) (IdempotencyRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return IdempotencyRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return IdempotencyRecord{}, false, err
	}
	now := time.Now().UTC()
	s.purgeExpiredIdempotencyRecordsLocked(now)
	record, ok := s.idempotencyRecords[idempotencyRecordIdentity(scope, key)]
	if !ok {
		return IdempotencyRecord{}, false, nil
	}
	return cloneIdempotencyRecord(record), true, ctx.Err()
}

func (s *LocalStore) purgeExpiredIdempotencyRecordsLocked(now time.Time) {
	for identity, record := range s.idempotencyRecords {
		if !record.ExpiresAt.After(now) {
			delete(s.idempotencyRecords, identity)
			s.idempotencyEvictions++
		}
	}
}

func (s *LocalStore) IdempotencyEvictions() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idempotencyEvictions
}

func (s *LocalStore) loadIdempotencyRecords(ctx context.Context) error {
	s.idempotencyRecords = map[string]IdempotencyRecord{}
	now := time.Now().UTC()
	seenCommits := make(map[[16]byte]struct{})
	var revision uint64
	for _, seg := range s.manifest.TxnSegments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := scanSegment(filepath.Join(s.path, seg), SegmentKindTxn, s.encryption, func(record scannedRecord) error {
			if record.header.kind != RecordKindTxnCommit {
				return nil
			}
			if _, ok := seenCommits[record.header.txnID]; !ok {
				seenCommits[record.header.txnID] = struct{}{}
				revision++
			}
			if len(record.payload) == 0 {
				return nil
			}
			var metadata txnCommitMetadata
			if err := json.Unmarshal(record.payload, &metadata); err != nil {
				return fmt.Errorf("%w: decode transaction commit metadata: %v", ErrInvalidRecord, err)
			}
			if metadata.Idempotency == nil {
				return nil
			}
			idempotency := cloneIdempotencyRecord(*metadata.Idempotency)
			if err := validateIdempotencyRecord(idempotency); err != nil {
				return err
			}
			if idempotency.StorageTxnID != record.header.txnID || idempotency.CommittedRevision != revision {
				return fmt.Errorf("%w: idempotency commit identity mismatch", ErrInvalidRecord)
			}
			if !idempotency.ExpiresAt.After(now) {
				s.idempotencyEvictions++
				return nil
			}
			s.idempotencyRecords[idempotencyRecordIdentity(idempotency.Scope, idempotency.Key)] = idempotency
			if len(s.idempotencyRecords) > MaxIdempotencyRecordsPerStore {
				return ErrIdempotencyCapacity
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if revision != s.revision {
		return fmt.Errorf("%w: idempotency revision scan mismatch", ErrInvalidRecord)
	}
	return nil
}
