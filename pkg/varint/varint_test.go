package varint_test

import (
	"testing"

	"github.com/thomastay/rash-db/pkg/varint"
)

func TestOneByteVarInt(t *testing.T) {
	for i := uint64(0); i <= 240; i++ {
		b := varint.Encode64(i)
		if len(b) != 1 {
			t.Errorf("%d: Length of b should be 1, got %d", i, len(b))
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
		key := uint16(b[0])*256 + uint16(b[1])
		if _, ok := seen[key]; ok {
			t.Errorf("%d: Duplicated key, %d, %d", i, b[0], b[1])
		}
		seen[key] = true
	}
}
