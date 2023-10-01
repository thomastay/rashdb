package varint_test

import (
	"testing"

	"github.com/thomastay/rash-db/pkg/varint"
)

func TestVarIntConsecutive(t *testing.T) {
	for i := uint64(0); i < 65536; i++ {
		varint.Encode64(i)
	}
}
