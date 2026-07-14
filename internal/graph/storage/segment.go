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
)

var segmentMagic = [4]byte{'K', 'S', 'E', 'G'}
var recordMagic = [4]byte{'K', 'R', 'E', 'C'}

const segmentVersion uint16 = 1
const recordVersion uint16 = 1
const segmentHeaderLen = 8
const recordHeaderLen = 48

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
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
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
	copy(buf[0:4], segmentMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], segmentVersion)
	buf[6] = byte(s.kind)
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
	if string(buf[0:4]) != string(segmentMagic[:]) || binary.BigEndian.Uint16(buf[4:6]) != segmentVersion || buf[6] != byte(s.kind) {
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
	copy(header[0:4], recordMagic[:])
	binary.BigEndian.PutUint16(header[4:6], recordVersion)
	header[6] = byte(kind)
	copy(header[8:24], txnID[:])
	copy(header[24:40], entityID[:])
	binary.BigEndian.PutUint32(header[40:44], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[44:48], crc32.ChecksumIEEE(payload))
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
	if string(h[0:4]) != string(segmentMagic[:]) || binary.BigEndian.Uint16(h[4:6]) != segmentVersion || h[6] != byte(kind) {
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
		if string(buf[0:4]) != string(recordMagic[:]) || binary.BigEndian.Uint16(buf[4:6]) != recordVersion {
			return fmt.Errorf("%w: bad record header at %s:%d", ErrInvalidRecord, path, off)
		}
		var txnID, entityID uuid.UUID
		copy(txnID[:], buf[8:24])
		copy(entityID[:], buf[24:40])
		l := binary.BigEndian.Uint32(buf[40:44])
		crc := binary.BigEndian.Uint32(buf[44:48])
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
		if err := visit(scannedRecord{header: recordHeader{kind: RecordKind(buf[6]), txnID: txnID, entityID: entityID, payloadLen: l, crc: crc}, location: RecordLocation{Segment: filepath.Base(path), Offset: off, Length: uint32(recordHeaderLen) + l}, payload: payload}); err != nil {
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
