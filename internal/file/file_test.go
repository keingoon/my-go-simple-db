package file

import (
	"fmt"
	"testing"
	"time"
)

func TestPage(t *testing.T) {
	t.Parallel()

	// Page has a fixed header region; offsets in Page APIs are relative to the data region.
	const maxDataOffset = 256 - 1 - pageHeaderSize

	t.Run("Page: ページ種別を取得できる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		if p.GetPageType() != heap {
			t.Errorf("ページ種別は%dであるべきだが%dだった", heap, p.GetPageType())
		}
	})

	t.Run("Page: ページLSNの初期値は0である", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		if p.GetPageLSN() != 0 {
			t.Errorf("pageLSNは%dであるべきだが%dだった", 0, p.GetPageLSN())
		}
	})

	t.Run("Page: ページLSNを設定するとエラーにならない", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		if err := p.SetPageLSN(1000); err != nil {
			t.Fatalf("エラーでないべきだが%vだった", err)
		}
	})

	t.Run("Page: SetPageLSNした値をGetPageLSNで取得できる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		if err := p.SetPageLSN(1000); err != nil {
			t.Fatalf("エラーでないべきだが%vだった", err)
		}
		if p.GetPageLSN() != 1000 {
			t.Errorf("pageLSNは%dであるべきだが%dだった", 1000, p.GetPageLSN())
		}
	})

	t.Run("Page: SetInt32/GetInt32（offset=0）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int32(1000)
			offset = int32(0)
		)
		p.SetInt32(offset, val)
		if p.GetInt32(offset) != val {
			t.Errorf("int32は%dであるべきだが%dだった", val, p.GetInt32(offset))
		}
	})

	t.Run("Page: SetInt32/GetInt32（offset=8）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int32(1000)
			offset = int32(8)
		)
		p.SetInt32(offset, val)
		if p.GetInt32(offset) != val {
			t.Errorf("int32は%dであるべきだが%dだった", val, p.GetInt32(offset))
		}
	})

	t.Run("Page: SetInt32は範囲外offsetだとエラーになる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int32(1000)
			offset = int32(254)
		)
		expectedErr := fmt.Errorf("set int32 offset out bound from 0 to %d", maxDataOffset)

		err := p.SetInt32(offset, val)
		if err == nil || err.Error() != expectedErr.Error() {
			t.Fatalf("エラーは%vであるべきだが%vだった", expectedErr, err)
		}
	})

	t.Run("Page: SetBytes/GetBytes（offset=0）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		val := []byte{104, 101, 108, 108, 111, 119, 111, 114, 108, 100}
		const (
			offset = int32(0)
		)
		p.SetBytes(offset, val)
		if got := p.GetBytes(offset); string(got) != "helloworld" {
			t.Errorf("bytesは%qであるべきだが%qだった", val, got)
		}
	})

	t.Run("Page: SetBytes/GetBytes（offset=8）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		val := []byte{104, 101, 108, 108, 111, 119, 111, 114, 108, 100}
		const offset = int32(8)
		p.SetBytes(offset, val)
		if got := p.GetBytes(offset); string(got) != "helloworld" {
			t.Errorf("bytesは%qであるべきだが%qだった", val, got)
		}
	})

	t.Run("Page: SetBytesは範囲外offsetだとエラーになる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		val := []byte{104, 101, 108, 108, 111, 119, 111, 114, 108, 100}
		const offset = int32(254)
		expectedErr := fmt.Errorf("set bytes offset out bound from 0 to %d", maxDataOffset)

		err := p.SetBytes(offset, val)
		if err == nil || err.Error() != expectedErr.Error() {
			t.Fatalf("エラーは%vであるべきだが%vだった", expectedErr, err)
		}
	})

	t.Run("Page: SetStr/GetStr（offset=0）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = "takeshitanaka"
			offset = int32(0)
		)
		p.SetStr(offset, val)
		if p.GetStr(offset) != val {
			t.Errorf("strは%qであるべきだが%qだった", val, p.GetStr(offset))
		}
	})

	t.Run("Page: SetStr/GetStr（offset=8）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = "takeshitanaka"
			offset = int32(8)
		)
		p.SetStr(offset, val)
		if p.GetStr(offset) != val {
			t.Errorf("strは%qであるべきだが%qだった", val, p.GetStr(offset))
		}
	})

	t.Run("Page: SetStrは範囲外offsetだとエラーになる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = "takeshitanaka"
			offset = int32(254)
		)
		var (
			setBytesErr = fmt.Errorf("set bytes offset out bound from 0 to %d", maxDataOffset)
			expectedErr = fmt.Errorf("set str err: %w", setBytesErr)
		)

		err := p.SetStr(offset, val)
		if err == nil || err.Error() != expectedErr.Error() {
			t.Errorf("エラーは%vであるべきだが%vだった", expectedErr, err)
		}
	})

	t.Run("Page: SetInt16/GetInt16（offset=0）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int16(1000)
			offset = int32(0)
		)
		p.SetInt16(offset, val)
		if p.GetInt16(offset) != val {
			t.Errorf("int16は%dであるべきだが%dだった", val, p.GetInt16(offset))
		}
	})

	t.Run("Page: SetInt16/GetInt16（offset=8）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int16(1000)
			offset = int32(8)
		)
		p.SetInt16(offset, val)
		if p.GetInt16(offset) != val {
			t.Errorf("int16は%dであるべきだが%dだった", val, p.GetInt16(offset))
		}
	})

	t.Run("Page: SetInt16は範囲外offsetだとエラーになる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int16(1000)
			offset = int32(255)
		)
		expectedErr := fmt.Errorf("set int16 offset out bound from 0 to %d", maxDataOffset)

		err := p.SetInt16(offset, val)
		if err == nil || err.Error() != expectedErr.Error() {
			t.Errorf("エラーは%vであるべきだが%vだった", expectedErr, err)
		}
	})

	t.Run("Page: SetBool/GetBool（true）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = true
			offset = int32(0)
		)
		p.SetBool(offset, val)
		if p.GetBool(offset) != val {
			t.Errorf("boolは%tであるべきだが%tだった", val, p.GetBool(offset))
		}
	})

	t.Run("Page: SetBool/GetBool（false）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = false
			offset = int32(8)
		)
		p.SetBool(offset, val)
		if p.GetBool(offset) != val {
			t.Errorf("boolは%tであるべきだが%tだった", val, p.GetBool(offset))
		}
	})

	t.Run("Page: SetBoolは範囲外offsetだとエラーになる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = true
			offset = int32(256)
		)
		expectedErr := fmt.Errorf("set bool offset out bound from 0 to %d", maxDataOffset)

		err := p.SetBool(offset, val)
		if err == nil || err.Error() != expectedErr.Error() {
			t.Errorf("エラーは%vであるべきだが%vだった", expectedErr, err)
		}
	})

	t.Run("Page: SetDate/GetDate（offset=0）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		var (
			val    = time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
			offset = int32(0)
		)
		p.SetDate(offset, val)
		if p.GetDate(offset) != val {
			t.Errorf("dateは%vであるべきだが%vだった", val, p.GetDate(offset))
		}
	})

	t.Run("Page: SetDate/GetDate（offset=8）", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		var (
			val    = time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
			offset = int32(8)
		)
		p.SetDate(offset, val)
		if p.GetDate(offset) != val {
			t.Errorf("dateは%vであるべきだが%vだった", val, p.GetDate(offset))
		}
	})

	t.Run("Page: SetDateは範囲外offsetだとエラーになる", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		var (
			val    = time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
			offset = int32(254)
		)
		expectedErr := fmt.Errorf("set date offset out bound from 0 to %d", maxDataOffset)

		err := p.SetDate(offset, val)
		if err == nil || err.Error() != expectedErr.Error() {
			t.Errorf("エラーは%vであるべきだが%vだった", expectedErr, err)
		}
	})

	t.Run("LogPage: NewLogPageで与えたバイト列が保持される", func(t *testing.T) {
		t.Parallel()
		const val = "<BEGIN>log<END>"
		b := []byte(val)
		p := NewLogPage(b)
		if string(p.bb) != val {
			t.Errorf("bbは%qであるべきだが%qだった", val, string(p.bb))
		}
	})
}

func TestFileMgr(t *testing.T) {
	t.Parallel()

	t.Run("FileMgr: Writeした値をReadで取得できる（offset=0）", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			offset    = int32(0)
			val       = "testtesttest"
			filename  = "testfile"
		)
		mgr, err := NewFileMgr(t.TempDir(), blocksize)
		if err != nil {
			t.Fatal(err)
		}
		writeP := NewPage(mgr.BlockSize())
		writeP.SetStr(offset, val)

		blk := NewBlockId(filename, 0)
		mgr.Write(blk, writeP)

		readP := NewPage(mgr.BlockSize())
		mgr.Read(blk, readP)
		if readP.GetStr(offset) != val {
			t.Errorf("文字列は%qであるべきだが%qだった", val, readP.GetStr(offset))
		}
	})

	t.Run("FileMgr: Writeした値をReadで取得できる（offsetあり）", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			offset    = int32(128)
			val       = "testtesttest"
			filename  = "testfile"
		)
		mgr, err := NewFileMgr(t.TempDir(), blocksize)
		if err != nil {
			t.Fatal(err)
		}
		writeP := NewPage(mgr.BlockSize())
		writeP.SetStr(offset, val)

		blk := NewBlockId(filename, 0)
		mgr.Write(blk, writeP)

		readP := NewPage(mgr.BlockSize())
		mgr.Read(blk, readP)
		if readP.GetStr(offset) != val {
			t.Errorf("文字列は%qであるべきだが%qだった", val, readP.GetStr(offset))
		}
	})

	t.Run("FileMgr: Appendするとブロック番号が1増える", func(t *testing.T) {
		t.Parallel()
		const (
			blocksize = int32(256)
			filename  = "testfile"
			offset    = 0
			val       = "testtesttest"
			blocknum  = 0
		)
		mgr, err := NewFileMgr(t.TempDir(), blocksize)
		if err != nil {
			t.Fatal(err)
		}
		blk := NewBlockId(filename, blocknum)
		writeP := NewPage(blocksize)
		writeP.SetStr(offset, val)
		mgr.Write(blk, writeP)

		newblk, err := mgr.Append(filename)
		if err != nil {
			t.Fatal(err)
		}

		if newblk == nil || newblk.FileName() != filename || newblk.Number() != blk.Number()+1 {
			gotFile, gotNum := "<nil>", int32(-1)
			if newblk != nil {
				gotFile, gotNum = newblk.FileName(), newblk.Number()
			}
			t.Fatalf("newblkは(%s, %d)であるべきだが(%s, %d)だった", filename, blk.Number()+1, gotFile, gotNum)
		}
	})

	t.Run("FileMgr: Length(filename)はブロック数を返す", func(t *testing.T) {
		t.Parallel()
		t.Run("未作成ファイルに対しては0を返す", func(t *testing.T) {
			t.Parallel()
			const (
				blocksize   = int32(256)
				filename    = "testfile"
				expectedlen = int32(0)
			)
			mgr, err := NewFileMgr(t.TempDir(), blocksize)
			if err != nil {
				t.Fatal(err)
			}
			gotLen, err := mgr.Length(filename)
			if err != nil {
				t.Fatal(err)
			}
			if gotLen != 0 {
				t.Errorf("Lengthは%dであるべきだが%dだった", expectedlen, gotLen)
			}
		})

		t.Run("書き込み後は1を返す", func(t *testing.T) {
			const (
				blocksize   = int32(256)
				offset      = int32(0)
				val         = "testtesttest"
				filename    = "testfile"
				expectedlen = int32(1)
			)
			mgr, err := NewFileMgr(t.TempDir(), blocksize)
			if err != nil {
				t.Fatal(err)
			}
			writeP := NewPage(mgr.BlockSize())
			writeP.SetStr(offset, val)
			blk := NewBlockId(filename, 0)
			mgr.Write(blk, writeP)
			gotLen, err := mgr.Length(filename)
			if err != nil {
				t.Fatal(err)
			}
			if gotLen != expectedlen {
				t.Errorf("Lengthは%dであるべきだが%dだった", expectedlen, gotLen)
			}
		})
	})
}
