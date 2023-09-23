package rashdb

import "testing"

func TestReadWriteHeaders(t *testing.T) {
	header := dbHeader{
		Version: 3,
	}
	b, err := header.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var readHeader dbHeader
	err = readHeader.UnmarshalBinary(b)
	if err != nil {
		t.Fatal(err)
	}
	if readHeader.Version != header.Version {
		t.Fatalf("Headers version not equal, expected %d, got %d", header.Version, readHeader.Version)
	}
	if readHeader.Magic != magicHeader {
		t.Fatalf("Magic header not set, got %d", readHeader.Magic)
	}
}
