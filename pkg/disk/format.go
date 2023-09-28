// Package disk contains the datastructures that are persisted to disk
// This can be used by other offline applications that want to manipulate the raw on-disk data structures.
package disk

import (
	"bytes"
	"encoding/binary"
)

// This is the file format that will be stored to disk
// The DB header is a 128 byte fixed size blob.
// Multi-byte structures are stored in Big endian format
type Header struct {
	// rashdb
	Magic   [16]byte
	Version uint32
}

const DBHeaderSize = 128
var MagicHeader = [16]byte{
	'r', 'a', 's', 'h', 'd', 'b', ' ',
	'f', 'o', 'r', 'm', 'a', 't', ' ',
	'A',
}

var dbHeaderOrder = binary.BigEndian

func (header *Header) MarshalBinary() (data []byte, err error) {
	b := fixedBytesBuffer{buf: make([]byte, DBHeaderSize)}

	if header.Magic[0] == 0 {
		// use default
		binary.Write(&b, dbHeaderOrder, MagicHeader)
	} else {
		binary.Write(&b, dbHeaderOrder, MagicHeader)
	}
	binary.Write(&b, dbHeaderOrder, header.Version)

	return b.Bytes(), nil
}

func (header *Header) UnmarshalBinary(data []byte) error {
	b := bytes.NewBuffer(data)
	err := binary.Read(b, dbHeaderOrder, header)
	if err != nil {
		return err
	}
	return nil
}

// Represents a table's columns, so we know what data goes into them.
// These are encoded into arrays and serialized as messagepack objects for simplicity
type Table struct {
	Name    string
	Columns []TableColumn
}

type TableColumn struct {
	Key   string
	Value DataType
}

//go:generate stringer -type=DataType
type DataType uint8

// Based on https://www.sqlite.org/datatype3.html
const (
	// Strings are likely to be the most common type, so they get 0
	DBStr DataType = iota
	DBInt
	DBReal
	DBNull
	DBText
	DBBlob
)
