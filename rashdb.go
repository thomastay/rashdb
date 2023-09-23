package rashdb

import (
	"encoding/binary"
	"fmt"
	"os"
)

const magicHeader uint32 = 0xDEADBEEF

type DB struct {
	path string
	file *os.File
}

// This is the file format that will be stored to disk
// The DB header is a 100 byte fixed size blob.
// Multi-byte structures are stored in Big endian format
type dbHeader struct {
	magic   uint32
	version uint32
}

var dbHeaderOrder = binary.BigEndian

func (header *dbHeader) MarshalBinary() (data []byte, err error) {
	b := fixedBytesBuffer{buf: make([]byte, 100)}

	if header.magic == 0 {
		// use default
		binary.Write(&b, dbHeaderOrder, magicHeader)
	} else {
		binary.Write(&b, dbHeaderOrder, magicHeader)
	}
	binary.Write(&b, dbHeaderOrder, header.version)

	return b.Bytes(), nil
}

func Open(filename string) (*DB, error) {
	header := dbHeader{
		version: 3,
	}
	b, err := header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	fmt.Println(b)

	return nil, nil
}
