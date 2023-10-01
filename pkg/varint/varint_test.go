package varint_test

import (
	"bytes"
	"testing"

	"github.com/thomastay/rash-db/pkg/varint"
)

func TestOneByteVarInt(t *testing.T) {
	for i := uint64(0); i <= 240; i++ {
		b := varint.Encode64(i)
		if len(b) != 1 {
			t.Errorf("%d: Length of b should be 1, got %d", i, len(b))
		}
		buf := bytes.NewBuffer(b)
		decoded, err := varint.Decode64(buf)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
		if decoded != i {
			t.Errorf("%d: Decoded %d", i, decoded)
		}
	}
}

func TestTwoByteVarInt(t *testing.T) {
	seen := make(map[uint16]bool)
	for i := uint64(241); i <= 2287; i++ {
		b := varint.Encode64(i)
		if len(b) != 2 {
			t.Errorf("%d: Length of b should be 2, got %d", i, len(b))
		}

		buf := bytes.NewBuffer(b)
		decoded, err := varint.Decode64(buf)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
		if decoded != i {
			t.Errorf("%d: Decoded %d", i, decoded)
		}

		key := uint16(b[0])*256 + uint16(b[1])
		if _, ok := seen[key]; ok {
			t.Errorf("%d: Duplicated key, %d, %d", i, b[0], b[1])
		}
		seen[key] = true
	}
}

func TestThreeByteVarInt(t *testing.T) {
	for i := uint64(2288); i <= 65535; i++ {
		b := varint.Encode64(i)
		if len(b) != 3 {
			t.Errorf("%d: Length of b should be 3, got %d", i, len(b))
		}
		buf := bytes.NewBuffer(b)
		decoded, err := varint.Decode64(buf)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
		if decoded != i {
			t.Errorf("%d: Decoded %d", i, decoded)
		}
	}
}

func TestBiggerVarIntQuick(t *testing.T) {
	seen := make(map[uint64]bool)
	for i := uint64(2288); i <= 100000; i++ {
		vint := varint.Encode64(i)

		key := uint64(0)
		for _, b := range vint {
			key = key << 8
			key += uint64(b)
		}

		if _, ok := seen[key]; ok {
			t.Errorf("%d: Duplicated key, %v", i, vint)
		}
		seen[key] = true
	}
}
