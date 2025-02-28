package file

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestPage(t *testing.T) {
	t.Parallel()

	t.Run("set int32 val 0 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int32(1000)
			offset = int32(0)
		)
		p.SetInt32(offset, val)
		if p.GetInt32(offset) != val {
			t.Errorf("expected %d, got %d", val, p.GetInt32(offset))
		}
	})

	t.Run("set int32 val 8 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int32(1000)
			offset = int32(8)
		)
		p.SetInt32(offset, val)
		if p.GetInt32(offset) != val {
			t.Errorf("expected %d, got %d", val, p.GetInt32(offset))
		}
	})

	t.Run("set int32 val 254 offset out of blocksize boundary 0 to 255", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int32(1000)
			offset = int32(254)
		)
		var expectedErr = fmt.Errorf("set int32 offset out bound from 0 to %d", 255)

		if err := p.SetInt32(offset, val); err.Error() != expectedErr.Error() {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("set bytes val 0 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		val := []byte{104, 101, 108, 108, 111, 119, 111, 114, 108, 100}
		const (
			offset = int32(0)
		)
		p.SetBytes(offset, val)
		if string(p.GetBytes(offset)) != "helloworld" {
			t.Errorf("expected %q, got %q", val, p.GetBytes(offset))
		}
	})

	t.Run("set bytes val 8 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		val := []byte{104, 101, 108, 108, 111, 119, 111, 114, 108, 100}
		const offset = int32(8)
		p.SetBytes(offset, val)
		if string(p.GetBytes(offset)) != "helloworld" {
			t.Errorf("expected %q, got %q", val, p.GetBytes(offset))
		}
	})

	t.Run("set bytes val 254 offset out of blocksize boundary 0 to 255", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		val := []byte{104, 101, 108, 108, 111, 119, 111, 114, 108, 100}
		const offset = int32(254)
		var expectedErr = fmt.Errorf("set bytes offset out bound from 0 to %d", 255)

		if err := p.SetBytes(offset, val); err.Error() != expectedErr.Error() {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("set str val 0 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = "takeshitanaka"
			offset = int32(0)
		)
		p.SetStr(offset, val)
		if p.GetStr(offset) != val {
			t.Errorf("expected %s, got %s", val, p.GetBytes(offset))
		}
	})

	t.Run("set str val 8 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = "takeshitanaka"
			offset = int32(0)
		)
		p.SetStr(offset, val)
		if p.GetStr(offset) != val {
			t.Errorf("expected %s, got %s", val, p.GetBytes(offset))
		}
	})

	t.Run("set str val 254 offset out of blocksize boundary 0 to 255", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = "takeshitanaka"
			offset = int32(254)
		)
		var (
			setBytesErr = fmt.Errorf("set bytes offset out bound from 0 to %d", 255)
			expectedErr = fmt.Errorf("set str err: %w", setBytesErr)
		)

		if err := p.SetStr(offset, val); err.Error() != expectedErr.Error() {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("set int16 val 0 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int16(1000)
			offset = int32(0)
		)
		p.SetInt16(offset, val)
		if p.GetInt16(offset) != val {
			t.Errorf("expected %d, got %d", val, p.GetInt16(offset))
		}
	})

	t.Run("set int16 val 8 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int16(1000)
			offset = int32(8)
		)
		p.SetInt16(offset, val)
		if p.GetInt16(offset) != val {
			t.Errorf("expected %d, got %d", val, p.GetInt16(offset))
		}
	})

	t.Run("set int16 val 255 offset out of blocksize boundary 0 to 255", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = int16(1000)
			offset = int32(255)
		)
		var expectedErr = fmt.Errorf("set int16 offset out bound from 0 to %d", 255)

		if err := p.SetInt16(offset, val); err.Error() != expectedErr.Error() {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("set bool true val 0 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = true
			offset = int32(0)
		)
		p.SetBool(offset, val)
		if p.GetBool(offset) != val {
			t.Errorf("expected %t, got %t", val, p.GetBool(offset))
		}
	})

	t.Run("set bool false val 8 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = false
			offset = int32(8)
		)
		p.SetBool(offset, val)
		if p.GetBool(offset) != val {
			t.Errorf("expected %t, got %t", val, p.GetBool(offset))
		}
	})

	t.Run("set bool true val 256 offset out of blocksize boundary 0 to 255", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		const (
			val    = true
			offset = int32(256)
		)
		var expectedErr = fmt.Errorf("set bool offset out bound from 0 to %d", 255)

		if err := p.SetBool(offset, val); err.Error() != expectedErr.Error() {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("set date val 0 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		var (
			val    = time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
			offset = int32(0)
		)
		p.SetDate(offset, val)
		if p.GetDate(offset) != val {
			t.Errorf("expected %s, got %s", val, p.GetDate(offset))
		}
	})

	t.Run("set date val 8 offset", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		var (
			val    = time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
			offset = int32(8)
		)
		p.SetDate(offset, val)
		if p.GetDate(offset) != val {
			t.Errorf("expected %s, got %s", val, p.GetDate(offset))
		}
	})

	t.Run("set date val 254 offset out of blocksize boundary 0 to 255", func(t *testing.T) {
		t.Parallel()
		p := NewPage(256)
		var (
			val    = time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
			offset = int32(254)
		)
		var expectedErr = fmt.Errorf("set date offset out bound from 0 to %d", 255)

		if err := p.SetDate(offset, val); err.Error() != expectedErr.Error() {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("call new log page", func(t *testing.T) {
		t.Parallel()
		const val = "<BEGIN>log<END>"
		b := []byte(val)
		p := NewLogPage(b)
		if string(p.bb) != val {
			t.Errorf("expected %s, got %s", val, string(p.bb))
		}
	})
}

func TestFileMgr(t *testing.T) {
	t.Parallel()

	t.Run("read and write", func(t *testing.T) {
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
			t.Errorf("expected %s, got %s", val, readP.GetStr(offset))
		}
	})

	t.Run("read and write with offset", func(t *testing.T) {
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
			t.Errorf("expected %s, got %s", val, readP.GetStr(offset))
		}
	})

	t.Run("Append", func(t *testing.T) {
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
		expectedblk := NewBlockId(filename, blk.Number()+1)
		if !reflect.DeepEqual(newblk, expectedblk) {
			t.Errorf("expected empty block %v, got %v", *expectedblk, *newblk)
		}
	})

	t.Run("Length", func(t *testing.T) {
		t.Parallel()
		t.Run("when new file", func(t *testing.T) {
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
			len, err := mgr.Length(filename)
			if err != nil {
				t.Fatal(err)
			}
			if len != 0 {
				t.Errorf("expected %d, got %d", expectedlen, len)
			}
		})

		t.Run("after write file", func(t *testing.T) {
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
			len, err := mgr.Length(filename)
			if err != nil {
				t.Fatal(err)
			}
			if len != expectedlen {
				t.Errorf("expected %d, got %d", expectedlen, len)
			}
		})
	})
}
