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
