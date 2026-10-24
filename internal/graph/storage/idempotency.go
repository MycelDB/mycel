package graphstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const MaxIdempotencyRecordsPerStore = 100_000

type txnCommitMetadata struct {
	Idempotency *IdempotencyRecord `json:"idempotency,omitempty"`
}

type idempotencyDeliveryMetadata struct {
	Scope  string `json:"scope"`
	Key    string `json:"key"`
	Digest string `json:"digest"`
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
	if record.PublicationAcknowledged && !record.RetainUntilAcknowledged {
		return fmt.Errorf("%w: publication acknowledgement requires retained idempotency", ErrInvalidRecord)
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

func idempotencyRecordExpired(record IdempotencyRecord, now time.Time) bool {
	if record.RetainUntilAcknowledged && !record.PublicationAcknowledged {
		return false
	}
	return !record.ExpiresAt.After(now)
}

func (s *LocalStore) purgeExpiredIdempotencyRecordsLocked(now time.Time) {
	for identity, record := range s.idempotencyRecords {
		if idempotencyRecordExpired(record, now) {
			delete(s.idempotencyRecords, identity)
			s.idempotencyEvictions++
		}
	}
}

func (s *LocalStore) ListIdempotencyRecords(ctx context.Context) ([]IdempotencyRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	s.purgeExpiredIdempotencyRecordsLocked(time.Now().UTC())
	records := make([]IdempotencyRecord, 0, len(s.idempotencyRecords))
	for _, record := range s.idempotencyRecords {
		records = append(records, cloneIdempotencyRecord(record))
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Scope == records[j].Scope {
			return records[i].Key < records[j].Key
		}
		return records[i].Scope < records[j].Scope
	})
	return records, ctx.Err()
}

func (s *LocalStore) RestoreIdempotencyRecords(ctx context.Context, records []IdempotencyRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return err
	}
	now := time.Now().UTC()
	s.purgeExpiredIdempotencyRecordsLocked(now)
	for _, record := range records {
		if idempotencyRecordExpired(record, now) {
			continue
		}
		if err := validateIdempotencyRecord(record); err != nil {
			return err
		}
		if record.StorageTxnID == uuid.Nil || record.CommittedRevision == 0 || record.CommittedRevision > s.revision {
			return fmt.Errorf("%w: restored idempotency commit identity is invalid", ErrInvalidRecord)
		}
		identity := idempotencyRecordIdentity(record.Scope, record.Key)
		if existing, ok := s.idempotencyRecords[identity]; ok && existing.Digest != record.Digest {
			return ErrIdempotencyConflict
		}
		if _, ok := s.idempotencyRecords[identity]; !ok && len(s.idempotencyRecords) >= MaxIdempotencyRecordsPerStore {
			return ErrIdempotencyCapacity
		}
		payload, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := s.txns.appendRecord(RecordKindIdempotencyRestore, record.StorageTxnID, uuid.Nil, payload); err != nil {
			return err
		}
		s.idempotencyRecords[identity] = cloneIdempotencyRecord(record)
	}
	return s.txns.sync()
}

func (s *LocalStore) MarkIdempotencyDelivered(ctx context.Context, scope, key, digest string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return err
	}
	identity := idempotencyRecordIdentity(scope, key)
	record, ok := s.idempotencyRecords[identity]
	if !ok {
		return ErrNotFound
	}
	if record.Digest != digest {
		return ErrIdempotencyConflict
	}
	if record.Delivered {
		return nil
	}
	payload, err := json.Marshal(idempotencyDeliveryMetadata{Scope: scope, Key: key, Digest: digest})
	if err != nil {
		return err
	}
	if _, err := s.txns.appendRecord(RecordKindIdempotencyDelivered, record.StorageTxnID, uuid.Nil, payload); err != nil {
		return err
	}
	if err := s.txns.sync(); err != nil {
		return err
	}
	record.Delivered = true
	s.idempotencyRecords[identity] = record
	return nil
}

func (s *LocalStore) RetainIdempotencyUntilAcknowledged(ctx context.Context, scope, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return err
	}
	identity := idempotencyRecordIdentity(scope, key)
	record, ok := s.idempotencyRecords[identity]
	if !ok {
		return ErrNotFound
	}
	if record.RetainUntilAcknowledged {
		return nil
	}
	payload, err := json.Marshal(idempotencyDeliveryMetadata{Scope: scope, Key: key, Digest: record.Digest})
	if err != nil {
		return err
	}
	if _, err := s.txns.appendRecord(RecordKindIdempotencyRetained, record.StorageTxnID, uuid.Nil, payload); err != nil {
		return err
	}
	if err := s.txns.sync(); err != nil {
		return err
	}
	record.RetainUntilAcknowledged = true
	s.idempotencyRecords[identity] = record
	return nil
}

func (s *LocalStore) MarkIdempotencyPublicationAcknowledged(ctx context.Context, scope, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureReady(); err != nil {
		return err
	}
	identity := idempotencyRecordIdentity(scope, key)
	record, ok := s.idempotencyRecords[identity]
	if !ok {
		return ErrNotFound
	}
	if !record.RetainUntilAcknowledged || record.PublicationAcknowledged {
		return nil
	}
	payload, err := json.Marshal(idempotencyDeliveryMetadata{Scope: scope, Key: key, Digest: record.Digest})
	if err != nil {
		return err
	}
	if _, err := s.txns.appendRecord(RecordKindIdempotencyAcknowledged, record.StorageTxnID, uuid.Nil, payload); err != nil {
		return err
	}
	if err := s.txns.sync(); err != nil {
		return err
	}
	record.PublicationAcknowledged = true
	s.idempotencyRecords[identity] = record
	return nil
}

func (s *LocalStore) IdempotencyStats() (active, capacity int, evictions uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredIdempotencyRecordsLocked(time.Now().UTC())
	return len(s.idempotencyRecords), MaxIdempotencyRecordsPerStore, s.idempotencyEvictions
}

func (s *LocalStore) IdempotencyEvictions() uint64 {
	_, _, evictions := s.IdempotencyStats()
	return evictions
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
			var idempotency *IdempotencyRecord
			switch record.header.kind {
			case RecordKindTxnCommit:
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
				idempotency = metadata.Idempotency
				if idempotency == nil {
					return nil
				}
				if idempotency.StorageTxnID != record.header.txnID || idempotency.CommittedRevision != revision {
					return fmt.Errorf("%w: idempotency commit identity mismatch", ErrInvalidRecord)
				}
			case RecordKindIdempotencyRestore:
				var restored IdempotencyRecord
				if err := json.Unmarshal(record.payload, &restored); err != nil {
					return fmt.Errorf("%w: decode restored idempotency metadata: %v", ErrInvalidRecord, err)
				}
				idempotency = &restored
				if restored.StorageTxnID != record.header.txnID || restored.CommittedRevision == 0 || restored.CommittedRevision > revision {
					return fmt.Errorf("%w: restored idempotency commit identity mismatch", ErrInvalidRecord)
				}
			case RecordKindIdempotencyDelivered, RecordKindIdempotencyAcknowledged, RecordKindIdempotencyRetained:
				var delivered idempotencyDeliveryMetadata
				if err := json.Unmarshal(record.payload, &delivered); err != nil {
					return fmt.Errorf("%w: decode idempotency delivery metadata: %v", ErrInvalidRecord, err)
				}
				identity := idempotencyRecordIdentity(delivered.Scope, delivered.Key)
				existing, ok := s.idempotencyRecords[identity]
				if !ok {
					return nil
				}
				if existing.Digest != delivered.Digest || existing.StorageTxnID != record.header.txnID {
					return fmt.Errorf("%w: delivered idempotency identity mismatch", ErrInvalidRecord)
				}
				switch record.header.kind {
				case RecordKindIdempotencyAcknowledged:
					existing.PublicationAcknowledged = true
				case RecordKindIdempotencyRetained:
					existing.RetainUntilAcknowledged = true
				default:
					existing.Delivered = true
				}
				s.idempotencyRecords[identity] = existing
				return nil
			default:
				return nil
			}
			candidate := cloneIdempotencyRecord(*idempotency)
			if err := validateIdempotencyRecord(candidate); err != nil {
				return err
			}
			// Defer expiry until all later retention/acknowledgement markers have
			// replayed. A legacy record may have expired by wall clock before its
			// durable promotion marker appears later in the WAL.
			s.idempotencyRecords[idempotencyRecordIdentity(candidate.Scope, candidate.Key)] = candidate
			return nil
		}); err != nil {
			return err
		}
	}
	if revision != s.revision {
		return fmt.Errorf("%w: idempotency revision scan mismatch", ErrInvalidRecord)
	}
	s.purgeExpiredIdempotencyRecordsLocked(now)
	if len(s.idempotencyRecords) > MaxIdempotencyRecordsPerStore {
		return ErrIdempotencyCapacity
	}
	return nil
}
