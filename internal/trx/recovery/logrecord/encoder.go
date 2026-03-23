package logrecord

import (
	"time"

	"github.com/keingoon/simpledb/internal/file"
)

type recordEncoder struct {
	rec []byte
	p   *file.Page
	pos int32
}

func newRecordEncoder(size int32) *recordEncoder {
	rec := make([]byte, size)
	return &recordEncoder{
		rec: rec,
		p:   file.NewLogPage(rec),
	}
}

func (enc *recordEncoder) Bytes() []byte {
	return enc.rec
}

func (enc *recordEncoder) PutInt32(v int32) error {
	if err := enc.p.SetInt32(enc.pos, v); err != nil {
		return err
	}
	enc.pos += int32Size
	return nil
}

func (enc *recordEncoder) PutInt16(v int16) error {
	if err := enc.p.SetInt16(enc.pos, v); err != nil {
		return err
	}
	enc.pos += int16Size
	return nil
}

func (enc *recordEncoder) PutBool(v bool) error {
	if err := enc.p.SetBool(enc.pos, v); err != nil {
		return err
	}
	enc.pos += boolSize
	return nil
}

func (enc *recordEncoder) PutDate(v time.Time) error {
	if err := enc.p.SetDate(enc.pos, v); err != nil {
		return err
	}
	enc.pos += dateSize
	return nil
}

func (enc *recordEncoder) PutStr(v string) error {
	if err := enc.p.SetStr(enc.pos, v); err != nil {
		return err
	}
	enc.pos += int32(file.VarBytesLen(len(v)))
	return nil
}

func (enc *recordEncoder) PutBlock(blk *file.BlockId) error {
	if err := enc.PutStr(blk.FileName()); err != nil {
		return err
	}
	if err := enc.PutInt32(blk.Number()); err != nil {
		return err
	}
	return nil
}
