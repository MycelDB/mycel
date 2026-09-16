package wal

import (
	"context"
	"io"
	"os"

	"github.com/myceldb/mycel/internal/encryption"
)

type Iterator struct {
	ctx        context.Context
	segments   []segmentInfo
	min        LSN
	idx        int
	file       *os.File
	closed     bool
	encryption *encryption.Service
}

func newIterator(ctx context.Context, segments []segmentInfo, min LSN, enc *encryption.Service) *Iterator {
	return &Iterator{ctx: ctx, segments: segments, min: min, encryption: enc}
}

func (it *Iterator) Next() (Record, bool, error) {
	if it.closed {
		return Record{}, false, ErrIteratorClosed
	}
	if err := it.ctx.Err(); err != nil {
		return Record{}, false, err
	}
	for {
		if it.file == nil {
			if it.idx >= len(it.segments) {
				return Record{}, false, nil
			}
			f, err := os.Open(it.segments[it.idx].path)
			if err != nil {
				return Record{}, false, err
			}
			it.file = f
		}
		rec, _, st, err := decodeFrame(it.file)
		if err != nil {
			return Record{}, false, err
		}
		switch st {
		case frameOK:
			if it.encryption != nil {
				payload, err := it.encryption.DecryptRecord(it.ctx, rec.Payload, walRecordAAD(rec))
				if err != nil {
					return Record{}, false, err
				}
				rec.Payload = payload
			}
			if rec.LSN < it.min {
				continue
			}
			return rec, true, nil
		case frameEOF:
			_ = it.file.Close()
			it.file = nil
			it.idx++
			continue
		case frameTorn:
			return Record{}, false, io.ErrUnexpectedEOF
		}
	}
}

func (it *Iterator) Close() error {
	it.closed = true
	if it.file != nil {
		err := it.file.Close()
		it.file = nil
		return err
	}
	return nil
}
