package record

import (
	"context"
	"fmt"
	"slices"

	"github.com/keingoon/simpledb/internal/file"
	"github.com/keingoon/simpledb/internal/trx/tx"
)

type FieldType int32

const (
	Integer FieldType = 4
	Varchar FieldType = 12
)

const (
	int32Size int32 = 4
)

type FieldInfo struct {
	fldtype FieldType
	length  int32
}

func NewFieldInfo(fldtype FieldType, length int32) *FieldInfo {
	return &FieldInfo{fldtype, length}
}

type Schema struct {
	fields []string
	info   map[string]*FieldInfo
}

func NewSchema() *Schema {
	return &Schema{fields: make([]string, 0), info: make(map[string]*FieldInfo)}
}

func (schema *Schema) Fields() []string {
	return schema.fields
}

func (schema *Schema) FldType(fldname string) FieldType {
	return schema.info[fldname].fldtype
}

func (schema *Schema) Length(fldname string) int32 {
	return schema.info[fldname].length
}

func (schema *Schema) HasField(fldname string) bool {
	return slices.Contains(schema.fields, fldname)
}

func (schema *Schema) AddField(fldname string, fldtype FieldType, length int32) {
	schema.fields = append(schema.fields, fldname)
	schema.info[fldname] = NewFieldInfo(fldtype, length)
}

func (schema *Schema) AddIntField(fldname string) {
	schema.AddField(fldname, Integer, 0)
}

func (schema *Schema) AddStringField(fldname string, length int32) {
	schema.AddField(fldname, Varchar, length)
}

func (schema *Schema) Add(fldname string, sch *Schema) {
	fldtype := sch.FldType(fldname)
	length := sch.Length(fldname)
	schema.AddField(fldname, fldtype, length)
}

func (schema *Schema) AddAll(sch *Schema) {
	for _, fldname := range sch.Fields() {
		schema.Add(fldname, sch)
	}
}

type Layout struct {
	schema   *Schema
	offsets  map[string]int32
	slotSize int32
}

func NewLayOut(schema *Schema) *Layout {
	offsets := make(map[string]int32)
	layout := &Layout{
		schema:  schema,
		offsets: offsets,
	}

	pos := int32Size
	for _, field := range schema.Fields() {
		offsets[field] = pos
		pos += layout.lengthInBytes(field)
	}
	layout.slotSize = pos
	return layout
}

func NewLayOutFromCatalog(schema *Schema, offsets map[string]int32, slotsize int32) *Layout {
	return &Layout{schema, offsets, slotsize}
}

func (layout *Layout) Schema() *Schema {
	return layout.schema
}

func (layout *Layout) Offset(fldname string) int32 {
	return layout.offsets[fldname]
}

func (layout *Layout) SlotSize() int32 {
	return layout.slotSize
}

func (layout *Layout) lengthInBytes(fldname string) int32 {
	fldType := layout.schema.FldType(fldname)
	if fldType == Integer {
		return int32Size
	}
	strLen := layout.schema.Length(fldname)
	return int32(file.VarBytesLen(int(strLen)))
}

const (
	Empty int32 = iota
	Used
)

type RecordPage struct {
	tx     *tx.TransactionMgr
	blk    *file.BlockId
	layout *Layout
}

func NewRecordPage(ctx context.Context, tx *tx.TransactionMgr, blk *file.BlockId, layout *Layout) *RecordPage {
	tx.Pin(ctx, blk)
	return &RecordPage{tx, blk, layout}
}

func (rp *RecordPage) GetInt(ctx context.Context, slot int32, fldname string) (int32, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetInt32(ctx, rp.blk, fldpos)
}

func (rp *RecordPage) GetString(ctx context.Context, slot int32, fldname string) (string, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetStr(ctx, rp.blk, fldpos)
}

func (rp *RecordPage) SetInt(ctx context.Context, slot int32, fldname string, val int32) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetInt32(ctx, rp.blk, fldpos, val, true)
}

func (rp *RecordPage) SetStr(ctx context.Context, slot int32, fldname string, val string) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetStr(ctx, rp.blk, fldpos, val, true)
}

func (rp *RecordPage) Delete(ctx context.Context, slot int32) error {
	return rp.setFlag(ctx, slot, Empty)
}

func (rp *RecordPage) Format(ctx context.Context) {
	slot := int32(0)
	for rp.isValidSlot(slot) {
		rp.tx.SetInt32(ctx, rp.blk, rp.offset(slot), Empty, false)
		sch := rp.layout.Schema()
		for _, fldname := range sch.Fields() {
			fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
			if sch.FldType(fldname) == Integer {
				rp.tx.SetInt32(ctx, rp.blk, fldpos, 0, false)
			} else if sch.FldType(fldname) == Varchar {
				rp.tx.SetStr(ctx, rp.blk, fldpos, "", false)
			}
		}
	}
}

func (rp *RecordPage) NextAfter(ctx context.Context, slot int32) (int32, error) {
	return rp.searchAfter(ctx, slot, Used)
}

func (rp *RecordPage) InsertAfter(ctx context.Context, slot int32) (int32, error) {
	newslot, err := rp.searchAfter(ctx, slot, Empty)
	if err != nil {
		return -1, fmt.Errorf("searchafter error: %w", err)
	}

	if err := rp.setFlag(ctx, newslot, Used); err != nil {
		return -1, fmt.Errorf("setflag error: %w", err)
	}

	return newslot, nil
}

func (rp *RecordPage) Block() *file.BlockId {
	return rp.blk
}

func (rp *RecordPage) setFlag(ctx context.Context, slot int32, flag int32) error {
	slotpos := rp.offset(slot)
	return rp.tx.SetInt32(ctx, rp.blk, slotpos, flag, true)
}

func (rp *RecordPage) searchAfter(ctx context.Context, slot int32, flag int32) (int32, error) {
	slot++
	for rp.isValidSlot(slot) {
		slotpos := rp.offset(slot)
		slotFlag, err := rp.tx.GetInt32(ctx, rp.blk, slotpos)
		if err != nil {
			return -1, fmt.Errorf("getint32 error: %w", err)
		}
		if slotFlag == flag {
			return slot, nil
		}
		slot++
	}
	return -1, nil
}

func (rp *RecordPage) isValidSlot(slot int32) bool {
	return rp.offset(slot+1) <= rp.tx.Blocksize()
}

func (rp *RecordPage) offset(slot int32) int32 {
	return slot * rp.layout.slotSize
}
