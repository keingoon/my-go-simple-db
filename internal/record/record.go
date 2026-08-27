package record

import (
	"slices"

	"github.com/keingoon/simpledb/internal/file"
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
