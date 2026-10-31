package record

import (
	"testing"

	"github.com/keingoon/simpledb/internal/file"
)

func TestSchema(t *testing.T) {
	t.Parallel()

	t.Run("Schema: NewSchemaの初期状態", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()

		t.Run("Fieldsは空である", func(t *testing.T) {
			t.Parallel()
			if got := len(sch.Fields()); got != 0 {
				t.Fatalf("Fieldsは空であるべきだが%d件だった", got)
			}
		})

		t.Run("HasFieldはfalseを返す", func(t *testing.T) {
			t.Parallel()
			if sch.HasField("A") {
				t.Fatalf("HasFieldはfalseを返すべき")
			}
		})
	})

	t.Run("Schema: AddIntField", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddIntField("A")

		t.Run("Fieldsにフィールド名が含まれる", func(t *testing.T) {
			t.Parallel()
			fields := sch.Fields()
			if len(fields) != 1 || fields[0] != "A" {
				t.Fatalf("Fieldsは[A]であるべきだが%vだった", fields)
			}
		})

		t.Run("FldTypeはIntegerを返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.FldType("A"); got != Integer {
				t.Fatalf("FldTypeは%dであるべきだが%dだった", Integer, got)
			}
		})

		t.Run("Lengthは0を返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.Length("A"); got != 0 {
				t.Fatalf("Lengthは0であるべきだが%dだった", got)
			}
		})

		t.Run("HasFieldはtrueを返す", func(t *testing.T) {
			t.Parallel()
			if !sch.HasField("A") {
				t.Fatalf("HasFieldはtrueを返すべき")
			}
		})
	})

	t.Run("Schema: AddSmallIntField", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddSmallIntField("A")

		t.Run("Fieldsにフィールド名が含まれる", func(t *testing.T) {
			t.Parallel()
			fields := sch.Fields()
			if len(fields) != 1 || fields[0] != "A" {
				t.Fatalf("Fieldsは[A]であるべきだが%vだった", fields)
			}
		})

		t.Run("FldTypeはSmallIntを返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.FldType("A"); got != SmallInt {
				t.Fatalf("FldTypeは%dであるべきだが%dだった", SmallInt, got)
			}
		})

		t.Run("Lengthは0を返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.Length("A"); got != 0 {
				t.Fatalf("Lengthは0であるべきだが%dだった", got)
			}
		})

		t.Run("HasFieldはtrueを返す", func(t *testing.T) {
			t.Parallel()
			if !sch.HasField("A") {
				t.Fatalf("HasFieldはtrueを返すべき")
			}
		})
	})

	t.Run("Schema: AddBoolField", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddBoolField("A")

		t.Run("Fieldsにフィールド名が含まれる", func(t *testing.T) {
			t.Parallel()
			fields := sch.Fields()
			if len(fields) != 1 || fields[0] != "A" {
				t.Fatalf("Fieldsは[A]であるべきだが%vだった", fields)
			}
		})

		t.Run("FldTypeはBooleanを返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.FldType("A"); got != Boolean {
				t.Fatalf("FldTypeは%dであるべきだが%dだった", Boolean, got)
			}
		})

		t.Run("Lengthは0を返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.Length("A"); got != 0 {
				t.Fatalf("Lengthは0であるべきだが%dだった", got)
			}
		})

		t.Run("HasFieldはtrueを返す", func(t *testing.T) {
			t.Parallel()
			if !sch.HasField("A") {
				t.Fatalf("HasFieldはtrueを返すべき")
			}
		})
	})

	t.Run("Schema: AddDateField", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddDateField("A")

		t.Run("Fieldsにフィールド名が含まれる", func(t *testing.T) {
			t.Parallel()
			fields := sch.Fields()
			if len(fields) != 1 || fields[0] != "A" {
				t.Fatalf("Fieldsは[A]であるべきだが%vだった", fields)
			}
		})

		t.Run("FldTypeはTimestampを返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.FldType("A"); got != Timestamp {
				t.Fatalf("FldTypeは%dであるべきだが%dだった", Timestamp, got)
			}
		})

		t.Run("Lengthは0を返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.Length("A"); got != 0 {
				t.Fatalf("Lengthは0であるべきだが%dだった", got)
			}
		})

		t.Run("HasFieldはtrueを返す", func(t *testing.T) {
			t.Parallel()
			if !sch.HasField("A") {
				t.Fatalf("HasFieldはtrueを返すべき")
			}
		})
	})

	t.Run("Schema: AddStringField", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddStringField("B", 9)

		t.Run("Fieldsにフィールド名が含まれる", func(t *testing.T) {
			t.Parallel()
			fields := sch.Fields()
			if len(fields) != 1 || fields[0] != "B" {
				t.Fatalf("Fieldsは[B]であるべきだが%vだった", fields)
			}
		})

		t.Run("FldTypeはVarcharを返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.FldType("B"); got != Varchar {
				t.Fatalf("FldTypeは%dであるべきだが%dだった", Varchar, got)
			}
		})

		t.Run("Lengthは指定した値を返す", func(t *testing.T) {
			t.Parallel()
			if got := sch.Length("B"); got != 9 {
				t.Fatalf("Lengthは9であるべきだが%dだった", got)
			}
		})
	})

	t.Run("Schema: Add", func(t *testing.T) {
		t.Parallel()
		src := NewSchema()
		src.AddIntField("A")
		src.AddStringField("B", 9)

		dst := NewSchema()
		dst.Add("A", src)
		dst.Add("B", src)

		t.Run("コピー先のFldTypeが一致する", func(t *testing.T) {
			t.Parallel()
			if got := dst.FldType("A"); got != Integer {
				t.Fatalf("FldType(A)は%dであるべきだが%dだった", Integer, got)
			}
			if got := dst.FldType("B"); got != Varchar {
				t.Fatalf("FldType(B)は%dであるべきだが%dだった", Varchar, got)
			}
		})

		t.Run("コピー先のLengthが一致する", func(t *testing.T) {
			t.Parallel()
			if got := dst.Length("A"); got != 0 {
				t.Fatalf("Length(A)は0であるべきだが%dだった", got)
			}
			if got := dst.Length("B"); got != 9 {
				t.Fatalf("Length(B)は9であるべきだが%dだった", got)
			}
		})
	})

	t.Run("Schema: AddAll", func(t *testing.T) {
		t.Parallel()
		src := NewSchema()
		src.AddIntField("A")
		src.AddStringField("B", 9)

		dst := NewSchema()
		dst.AddAll(src)

		t.Run("全フィールドがコピーされる", func(t *testing.T) {
			t.Parallel()
			fields := dst.Fields()
			if len(fields) != 2 {
				t.Fatalf("Fieldsは2件であるべきだが%d件だった", len(fields))
			}
			if fields[0] != "A" || fields[1] != "B" {
				t.Fatalf("Fieldsは[A B]であるべきだが%vだった", fields)
			}
		})

		t.Run("型と長さが保持される", func(t *testing.T) {
			t.Parallel()
			if got := dst.FldType("A"); got != Integer {
				t.Fatalf("FldType(A)は%dであるべきだが%dだった", Integer, got)
			}
			if got := dst.Length("B"); got != 9 {
				t.Fatalf("Length(B)は9であるべきだが%dだった", got)
			}
		})
	})
}

func TestLayout(t *testing.T) {
	t.Parallel()

	t.Run("Layout: int1フィールドのoffsetとslotSize", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddIntField("A")
		layout := NewLayOut(sch)

		t.Run("Offset(A)は4である", func(t *testing.T) {
			t.Parallel()
			// 先頭4バイトはempty/inuseフラグ
			if got := layout.Offset("A"); got != 4 {
				t.Fatalf("Offset(A)は4であるべきだが%dだった", got)
			}
		})

		t.Run("SlotSizeは8である", func(t *testing.T) {
			t.Parallel()
			// flag(4) + int(4)
			if got := layout.SlotSize(); got != 8 {
				t.Fatalf("SlotSizeは8であるべきだが%dだった", got)
			}
		})
	})

	t.Run("Layout: smallintフィールドのoffsetとslotSize", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddSmallIntField("A")
		layout := NewLayOut(sch)

		t.Run("Offset(A)は4である", func(t *testing.T) {
			t.Parallel()
			// 先頭4バイトはempty/inuseフラグ
			if got := layout.Offset("A"); got != 4 {
				t.Fatalf("Offset(A)は4であるべきだが%dだった", got)
			}
		})

		t.Run("SlotSizeは6である", func(t *testing.T) {
			t.Parallel()
			// flag(4) + smallint(2)
			if got := layout.SlotSize(); got != 6 {
				t.Fatalf("SlotSizeは6であるべきだが%dだった", got)
			}
		})
	})

	t.Run("Layout: booleanフィールドのoffsetとslotSize", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddBoolField("A")
		layout := NewLayOut(sch)

		t.Run("Offset(A)は4である", func(t *testing.T) {
			t.Parallel()
			// 先頭4バイトはempty/inuseフラグ
			if got := layout.Offset("A"); got != 4 {
				t.Fatalf("Offset(A)は4であるべきだが%dだった", got)
			}
		})

		t.Run("SlotSizeは5である", func(t *testing.T) {
			t.Parallel()
			// flag(4) + bool(1)
			if got := layout.SlotSize(); got != 5 {
				t.Fatalf("SlotSizeは5であるべきだが%dだった", got)
			}
		})
	})

	t.Run("Layout: timestampフィールドのoffsetとslotSize", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddDateField("A")
		layout := NewLayOut(sch)

		t.Run("Offset(A)は4である", func(t *testing.T) {
			t.Parallel()
			// 先頭4バイトはempty/inuseフラグ
			if got := layout.Offset("A"); got != 4 {
				t.Fatalf("Offset(A)は4であるべきだが%dだった", got)
			}
		})

		t.Run("SlotSizeは12である", func(t *testing.T) {
			t.Parallel()
			// flag(4) + timestamp(8)
			if got := layout.SlotSize(); got != 12 {
				t.Fatalf("SlotSizeは12であるべきだが%dだった", got)
			}
		})
	})

	t.Run("Layout: intとvarchar(9)のoffsetとslotSize", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddIntField("A")
		sch.AddStringField("B", 9)
		layout := NewLayOut(sch)

		t.Run("Offset(A)は4である", func(t *testing.T) {
			t.Parallel()
			if got := layout.Offset("A"); got != 4 {
				t.Fatalf("Offset(A)は4であるべきだが%dだった", got)
			}
		})

		t.Run("Offset(B)は8である", func(t *testing.T) {
			t.Parallel()
			// flag(4) + int(4)
			if got := layout.Offset("B"); got != 8 {
				t.Fatalf("Offset(B)は8であるべきだが%dだった", got)
			}
		})

		t.Run("SlotSizeは21である", func(t *testing.T) {
			t.Parallel()
			// flag(4) + int(4) + VarBytesLen(9)=13
			want := int32(4 + 4 + file.VarBytesLen(9))
			if got := layout.SlotSize(); got != want {
				t.Fatalf("SlotSizeは%dであるべきだが%dだった", want, got)
			}
		})
	})

	t.Run("Layout: NewLayOutFromCatalogは渡した値をそのまま使う", func(t *testing.T) {
		t.Parallel()
		sch := NewSchema()
		sch.AddIntField("A")
		sch.AddStringField("B", 9)
		offsets := map[string]int32{"A": 4, "B": 8}
		const slotSize int32 = 21
		layout := NewLayOutFromCatalog(sch, offsets, slotSize)

		t.Run("Offsetは渡した値を返す", func(t *testing.T) {
			t.Parallel()
			if got := layout.Offset("A"); got != 4 {
				t.Fatalf("Offset(A)は4であるべきだが%dだった", got)
			}
			if got := layout.Offset("B"); got != 8 {
				t.Fatalf("Offset(B)は8であるべきだが%dだった", got)
			}
		})

		t.Run("SlotSizeは渡した値を返す", func(t *testing.T) {
			t.Parallel()
			if got := layout.SlotSize(); got != slotSize {
				t.Fatalf("SlotSizeは%dであるべきだが%dだった", slotSize, got)
			}
		})
	})
}

func TestRID(t *testing.T) {
	t.Parallel()

	t.Run("RID: NewRIDの初期状態", func(t *testing.T) {
		t.Parallel()
		const (
			blknum int32 = 4
			slot   int32 = 3
		)
		rid := NewRID(blknum, slot)

		t.Run("BlockNumberは指定の値である", func(t *testing.T) {
			t.Parallel()
			if got := rid.BlockNumber(); got != blknum {
				t.Fatalf("BlockNumberは%dであるべきだが%dだった", blknum, got)
			}
		})

		t.Run("Slotは指定の値である", func(t *testing.T) {
			t.Parallel()
			if got := rid.Slot(); got != slot {
				t.Fatalf("Slotは%dであるべきだが%dだった", slot, got)
			}
		})
	})

	t.Run("RID: Equals", func(t *testing.T) {
		t.Parallel()
		const (
			blknum1 int32 = 4
			slot1   int32 = 3
			blknum2 int32 = 5
			slot2   int32 = 6
		)
		t.Run("同じblknumとslotならtrueを返す", func(t *testing.T) {
			t.Parallel()
			rid1 := NewRID(blknum1, slot1)
			rid2 := NewRID(blknum1, slot1)
			if got := rid1.Equals(rid2); got != true {
				t.Fatalf("Equalsは%tであるべきだが%tだった", true, got)
			}
		})
		t.Run("blknumが異なればfalseを返す", func(t *testing.T) {
			t.Parallel()
			rid1 := NewRID(blknum1, slot1)
			rid2 := NewRID(blknum2, slot1)
			if got := rid1.Equals(rid2); got != false {
				t.Fatalf("Equalsは%tであるべきだが%tだった", false, got)
			}
		})
		t.Run("slotが異なればfalseを返す", func(t *testing.T) {
			t.Parallel()
			rid1 := NewRID(blknum1, slot1)
			rid2 := NewRID(blknum1, slot2)
			if got := rid1.Equals(rid2); got != false {
				t.Fatalf("Equalsは%tであるべきだが%tだった", false, got)
			}
		})
	})

	t.Run("RID: ToString", func(t *testing.T) {
		t.Parallel()
		const (
			blknum int32 = 4
			slot   int32 = 3
		)
		rid := NewRID(blknum, slot)

		if got := rid.ToString(); got != "[4, 3]" {
			t.Fatalf("ToStringは[4, 3]であるべきだが%qだった", got)
		}
	})
}
