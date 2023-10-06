// Package disk contains the datastructures that are persisted to disk
// This can be used by other offline applications that want to manipulate the raw on-disk data structures.
package disk

import (
	"bytes"
	"encoding/binary"

	"github.com/thomastay/rash-db/pkg/common"
)

// This is the file format that will be stored to disk
// The DB header is a 128 byte fixed size blob.
// Multi-byte structures are stored in Big endian format
type Header struct {
	// rashdb
	Magic    [16]byte
	Version  uint32
	PageSize uint16
}

const DBHeaderSize = 128
const DefaultDBPageSize = 4096

var MagicHeader = [16]byte{
	'r', 'a', 's', 'h', 'd', 'b', ' ',
	'f', 'o', 'r', 'm', 'a', 't', ' ',
	'A',
}

var dbEndianness = binary.BigEndian

func (header *Header) MarshalBinary() (data []byte, err error) {
	b := NewFixedBytesBuffer(make([]byte, DBHeaderSize))

	if header.Magic[0] == 0 {
		// use default
		common.Check(binary.Write(b, dbEndianness, MagicHeader))
	} else {
		common.Check(binary.Write(b, dbEndianness, header.Magic))
	}
	common.Check(binary.Write(b, dbEndianness, header.Version))
	if header.PageSize == 0 {
		common.Check(binary.Write(b, dbEndianness, DefaultDBPageSize))
	} else {
		common.Check(binary.Write(b, dbEndianness, header.PageSize))
	}

	return b.Bytes(), nil
}

func (header *Header) UnmarshalBinary(data []byte) error {
	b := bytes.NewBuffer(data)
	err := binary.Read(b, dbEndianness, header)
	if err != nil {
		return err
	}
	return nil
}
