package rashdb_test

import (
	"testing"

	"github.com/thomastay/rash-db/pkg/disk"
)

func TestReadWriteHeaders(t *testing.T) {
	header := disk.Header{
		Version: 3,
	}
	b, err := header.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var readHeader disk.Header
	err = readHeader.UnmarshalBinary(b)
	if err != nil {
		t.Fatal(err)
	}
	if readHeader.Version != header.Version {
		t.Fatalf("Headers version not equal, expected %d, got %d", header.Version, readHeader.Version)
	}
	if readHeader.Magic != disk.MagicHeader {
		t.Fatalf("Magic header not set, got %d", readHeader.Magic)
	}
}
