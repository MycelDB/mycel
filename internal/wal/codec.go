package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

const (
	frameMagic   uint32 = 0x4d57414c // MWAL
	frameVersion uint16 = 1

	frameMagicLen   = 4
	frameVersionLen = 2
	frameLengthLen  = 4
	frameCRCLen     = 4

	frameMagicOffset   = 0
	frameVersionOffset = frameMagicOffset + frameMagicLen
	frameLengthOffset  = frameVersionOffset + frameVersionLen
	frameHeaderLen     = frameLengthOffset + frameLengthLen

	maxRecordTypeLen = 0xffff
	maxFrameLen      = 128 * 1024 * 1024
)

const (
	frameBodyLSNLen           = 8
	frameBodyTimestampLen     = 8
	frameBodySchemaVersionLen = 2
	frameBodyEncodingLen      = 1
	frameBodyTypeLenLen       = 2
	frameBodyPayloadLenLen    = 4

	frameBodyLSNOffset           = 0
	frameBodyTimestampOffset     = frameBodyLSNOffset + frameBodyLSNLen
	frameBodySchemaVersionOffset = frameBodyTimestampOffset + frameBodyTimestampLen
	frameBodyEncodingOffset      = frameBodySchemaVersionOffset + frameBodySchemaVersionLen
	frameBodyTypeLenOffset       = frameBodyEncodingOffset + frameBodyEncodingLen
	frameBodyTypeOffset          = frameBodyTypeLenOffset + frameBodyTypeLenLen

	frameBodyFixedLen = frameBodyTypeOffset + frameBodyPayloadLenLen
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type frameStatus int

const (
	frameOK frameStatus = iota
	frameEOF
	frameTorn
)

func encodeFrame(rec Record) ([]byte, error) {
	if rec.LSN == 0 || rec.Type == "" || rec.SchemaVersion == 0 || len(rec.Payload) == 0 {
		return nil, ErrInvalidRecord
	}
	if rec.Encoding == 0 {
		rec.Encoding = PayloadEncodingJSON
	}
	typeBytes := []byte(rec.Type)
	if len(typeBytes) > maxRecordTypeLen {
		return nil, fmt.Errorf("%w: record type too long", ErrInvalidRecord)
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
	payloadLen := len(rec.Payload)
	bodyLen := frameBodyFixedLen + len(typeBytes) + payloadLen
	if bodyLen+frameCRCLen > maxFrameLen {
		return nil, fmt.Errorf("%w: frame too large", ErrInvalidRecord)
	}
	buf := bytes.NewBuffer(make([]byte, 0, frameHeaderLen+bodyLen+frameCRCLen))
	_ = binary.Write(buf, binary.BigEndian, frameMagic)
	_ = binary.Write(buf, binary.BigEndian, uint16(frameVersion))
	_ = binary.Write(buf, binary.BigEndian, uint32(bodyLen+frameCRCLen)) // body + crc
	_ = binary.Write(buf, binary.BigEndian, uint64(rec.LSN))
	_ = binary.Write(buf, binary.BigEndian, uint64(rec.Timestamp.UTC().UnixNano()))
	_ = binary.Write(buf, binary.BigEndian, rec.SchemaVersion)
	buf.WriteByte(byte(rec.Encoding))
	_ = binary.Write(buf, binary.BigEndian, uint16(len(typeBytes)))
	buf.Write(typeBytes)
	_ = binary.Write(buf, binary.BigEndian, uint32(payloadLen))
	buf.Write(rec.Payload)
	crc := crc32.Checksum(buf.Bytes(), crcTable)
	_ = binary.Write(buf, binary.BigEndian, crc)
	return buf.Bytes(), nil
}

func decodeFrame(r io.Reader) (Record, int64, frameStatus, error) {
	header := make([]byte, frameHeaderLen)
	n, err := io.ReadFull(r, header)
	if err == io.EOF && n == 0 {
		return Record{}, 0, frameEOF, nil
	}
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return Record{}, int64(n), frameTorn, nil
	}
	if err != nil {
		return Record{}, int64(n), frameEOF, err
	}
	if binary.BigEndian.Uint32(header[frameMagicOffset:frameVersionOffset]) != frameMagic || binary.BigEndian.Uint16(header[frameVersionOffset:frameLengthOffset]) != frameVersion {
		return Record{}, int64(n), frameOK, fmt.Errorf("%w: bad frame header", ErrCorrupt)
	}
	frameLen := binary.BigEndian.Uint32(header[frameLengthOffset:frameHeaderLen])
	if frameLen < frameCRCLen || frameLen > maxFrameLen {
		return Record{}, int64(n), frameOK, fmt.Errorf("%w: invalid frame length", ErrCorrupt)
	}
	bodyCRC := make([]byte, frameLen)
	n2, err := io.ReadFull(r, bodyCRC)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return Record{}, int64(n + n2), frameTorn, nil
	}
	if err != nil {
		return Record{}, int64(n + n2), frameEOF, err
	}
	got := binary.BigEndian.Uint32(bodyCRC[len(bodyCRC)-frameCRCLen:])
	want := crc32.Checksum(append(header, bodyCRC[:len(bodyCRC)-frameCRCLen]...), crcTable)
	if got != want {
		return Record{}, int64(n + n2), frameOK, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	b := bodyCRC[:len(bodyCRC)-frameCRCLen]
	if len(b) < frameBodyFixedLen {
		return Record{}, int64(n + n2), frameOK, fmt.Errorf("%w: short body", ErrCorrupt)
	}
	rec := Record{}
	rec.LSN = LSN(binary.BigEndian.Uint64(b[frameBodyLSNOffset:frameBodyTimestampOffset]))
	rec.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(b[frameBodyTimestampOffset:frameBodySchemaVersionOffset]))).UTC()
	rec.SchemaVersion = binary.BigEndian.Uint16(b[frameBodySchemaVersionOffset:frameBodyEncodingOffset])
	rec.Encoding = PayloadEncoding(b[frameBodyEncodingOffset])
	typeLen := int(binary.BigEndian.Uint16(b[frameBodyTypeLenOffset:frameBodyTypeOffset]))
	pos := frameBodyTypeOffset
	if len(b) < pos+typeLen+frameBodyPayloadLenLen {
		return Record{}, int64(n + n2), frameOK, fmt.Errorf("%w: short type", ErrCorrupt)
	}
	rec.Type = RecordType(string(b[pos : pos+typeLen]))
	pos += typeLen
	payloadLen := int(binary.BigEndian.Uint32(b[pos : pos+frameBodyPayloadLenLen]))
	pos += frameBodyPayloadLenLen
	if len(b) != pos+payloadLen {
		return Record{}, int64(n + n2), frameOK, fmt.Errorf("%w: payload length mismatch", ErrCorrupt)
	}
	rec.Payload = append([]byte(nil), b[pos:]...)
	return rec, int64(n + n2), frameOK, nil
}
