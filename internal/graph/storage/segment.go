package graphstorage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/myceldb/mycel/internal/fsperm"
)

var segmentMagic = [4]byte{'K', 'S', 'E', 'G'}
var recordMagic = [4]byte{'K', 'R', 'E', 'C'}

const segmentVersion uint16 = 1
const recordVersion uint16 = 1

const (
	magicLen    = 4
	versionLen  = 2
	kindLen     = 1
	reservedLen = 1
	uuidLen     = 16
	uint32Len   = 4
)

const (
	segmentMagicOffset   = 0
	segmentVersionOffset = segmentMagicOffset + magicLen
	segmentKindOffset    = segmentVersionOffset + versionLen
	segmentHeaderLen     = segmentKindOffset + kindLen + reservedLen
)

const (
	recordMagicOffset      = 0
	recordVersionOffset    = recordMagicOffset + magicLen
	recordKindOffset       = recordVersionOffset + versionLen
	recordTxnIDOffset      = recordKindOffset + kindLen + reservedLen
	recordEntityIDOffset   = recordTxnIDOffset + uuidLen
	recordPayloadLenOffset = recordEntityIDOffset + uuidLen
	recordCRCOffset        = recordPayloadLenOffset + uint32Len
	recordHeaderLen        = recordCRCOffset + uint32Len
)

type segment struct {
	id   string
	path string
	kind SegmentKind
	file *os.File
}
type recordHeader struct {
	kind       RecordKind
	txnID      uuid.UUID
	entityID   uuid.UUID
	payloadLen uint32
	crc        uint32
}
type scannedRecord struct {
	header   recordHeader
	location RecordLocation
	payload  []byte
}

func openSegment(path string, kind SegmentKind) (*segment, error) {
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return nil, err
	}
	exists := true
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			exists = false
		} else {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, fsperm.PrivateFile)
	if err != nil {
		return nil, err
	}
	s := &segment{id: filepath.Base(path), path: path, kind: kind, file: f}
	if !exists || fileSize(f) == 0 {
		if err := s.writeHeader(); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else if err := s.verifyHeader(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

func (s *segment) writeHeader() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, segmentHeaderLen)
	copy(buf[segmentMagicOffset:segmentVersionOffset], segmentMagic[:])
	binary.BigEndian.PutUint16(buf[segmentVersionOffset:segmentKindOffset], segmentVersion)
	buf[segmentKindOffset] = byte(s.kind)
	_, err := s.file.Write(buf)
	return err
}
func (s *segment) verifyHeader() error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, segmentHeaderLen)
	if _, err := io.ReadFull(f, buf); err != nil {
		return err
	}
	if string(buf[segmentMagicOffset:segmentVersionOffset]) != string(segmentMagic[:]) || binary.BigEndian.Uint16(buf[segmentVersionOffset:segmentKindOffset]) != segmentVersion || buf[segmentKindOffset] != byte(s.kind) {
		return fmt.Errorf("%w: bad segment header %s", ErrInvalidRecord, s.path)
	}
	return nil
}
func (s *segment) appendRecord(kind RecordKind, txnID, entityID uuid.UUID, payload []byte) (RecordLocation, error) {
	off, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return RecordLocation{}, err
	}
	header := make([]byte, recordHeaderLen)
	copy(header[recordMagicOffset:recordVersionOffset], recordMagic[:])
	binary.BigEndian.PutUint16(header[recordVersionOffset:recordKindOffset], recordVersion)
	header[recordKindOffset] = byte(kind)
	copy(header[recordTxnIDOffset:recordEntityIDOffset], txnID[:])
	copy(header[recordEntityIDOffset:recordPayloadLenOffset], entityID[:])
	binary.BigEndian.PutUint32(header[recordPayloadLenOffset:recordCRCOffset], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[recordCRCOffset:recordHeaderLen], crc32.ChecksumIEEE(payload))
	if _, err := s.file.Write(header); err != nil {
		return RecordLocation{}, err
	}
	if len(payload) > 0 {
		if _, err := s.file.Write(payload); err != nil {
			return RecordLocation{}, err
		}
	}
	return RecordLocation{Segment: s.id, Offset: off, Length: uint32(recordHeaderLen + len(payload))}, nil
}
func (s *segment) sync() error  { return s.file.Sync() }
func (s *segment) close() error { return s.file.Close() }

func scanSegment(path string, kind SegmentKind, visit func(scannedRecord) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := make([]byte, segmentHeaderLen)
	if _, err := io.ReadFull(f, h); err != nil {
		return err
	}
	if string(h[segmentMagicOffset:segmentVersionOffset]) != string(segmentMagic[:]) || binary.BigEndian.Uint16(h[segmentVersionOffset:segmentKindOffset]) != segmentVersion || h[segmentKindOffset] != byte(kind) {
		return fmt.Errorf("%w: bad segment header %s", ErrInvalidRecord, path)
	}
	for {
		off, _ := f.Seek(0, io.SeekCurrent)
		buf := make([]byte, recordHeaderLen)
		_, err := io.ReadFull(f, buf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		if string(buf[recordMagicOffset:recordVersionOffset]) != string(recordMagic[:]) || binary.BigEndian.Uint16(buf[recordVersionOffset:recordKindOffset]) != recordVersion {
			return fmt.Errorf("%w: bad record header at %s:%d", ErrInvalidRecord, path, off)
		}
		var txnID, entityID uuid.UUID
		copy(txnID[:], buf[recordTxnIDOffset:recordEntityIDOffset])
		copy(entityID[:], buf[recordEntityIDOffset:recordPayloadLenOffset])
		l := binary.BigEndian.Uint32(buf[recordPayloadLenOffset:recordCRCOffset])
		crc := binary.BigEndian.Uint32(buf[recordCRCOffset:recordHeaderLen])
		payload := make([]byte, l)
		if l > 0 {
			if _, err := io.ReadFull(f, payload); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil
				}
				return err
			}
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return fmt.Errorf("%w: bad crc at %s:%d", ErrInvalidRecord, path, off)
		}
		if err := visit(scannedRecord{header: recordHeader{kind: RecordKind(buf[recordKindOffset]), txnID: txnID, entityID: entityID, payloadLen: l, crc: crc}, location: RecordLocation{Segment: filepath.Base(path), Offset: off, Length: uint32(recordHeaderLen) + l}, payload: payload}); err != nil {
			return err
		}
	}
}
func fileSize(f *os.File) int64 {
	st, err := f.Stat()
	if err != nil {
		return 0
	}
	return st.Size()
}
