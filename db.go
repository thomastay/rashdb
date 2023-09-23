package rashdb

import (
	"bytes"
	"encoding/binary"
)

// This is the file format that will be stored to disk
// The DB header is a 100 byte fixed size blob.
// Multi-byte structures are stored in Big endian format
type dbHeader struct {
	Magic   uint32
	Version uint32
}

const magicHeader uint32 = 0xDEADBEEF
const dbHeaderSize = 100

var dbHeaderOrder = binary.BigEndian

func (header *dbHeader) MarshalBinary() (data []byte, err error) {
	b := fixedBytesBuffer{buf: make([]byte, dbHeaderSize)}

	if header.Magic == 0 {
		// use default
		binary.Write(&b, dbHeaderOrder, magicHeader)
	} else {
		binary.Write(&b, dbHeaderOrder, magicHeader)
	}
	binary.Write(&b, dbHeaderOrder, header.Version)

	return b.Bytes(), nil
}
func (header *dbHeader) UnmarshalBinary(data []byte) error {
	b := bytes.NewBuffer(data)
	err := binary.Read(b, dbHeaderOrder, header)
	if err != nil {
		return err
	}
	return nil
}
