// Package disk contains the datastructures that are persisted to disk
// This can be used by other offline applications that want to manipulate the raw on-disk data structures.
package disk

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/varint"
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

var dbHeaderOrder = binary.BigEndian

func (header *Header) MarshalBinary() (data []byte, err error) {
	b := NewFixedBytesBuffer(make([]byte, DBHeaderSize))

	if header.Magic[0] == 0 {
		// use default
		binary.Write(b, dbHeaderOrder, MagicHeader)
	} else {
		binary.Write(b, dbHeaderOrder, header.Magic)
	}
	binary.Write(b, dbHeaderOrder, header.Version)
	if header.PageSize == 0 {
		binary.Write(b, dbHeaderOrder, DefaultDBPageSize)
	} else {
		binary.Write(b, dbHeaderOrder, header.PageSize)
	}

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
	Name       string
	PrimaryKey []TableColumn
	// Note: These columns don't contain the primary key(s)
	Columns []TableColumn
}

type TableColumn struct {
	Key   string
	Value DataType
}

type KeyValueLen struct {
	KeyLen uint32
	ValLen uint32
}

type KeyValue struct {
	// Keys and values are stored as opaque structs and decoded as needed
	Key []byte
	Val []byte
}

func ReadKV(r io.Reader) (*KeyValue, error) {
	keyLen, err := varint.Decode(r)
	if err != nil {
		return nil, err
	}
	valLen, err := varint.Decode(r)
	if err != nil {
		return nil, err
	}
	kv := KeyValue{}
	kv.Key, err = common.ReadExactly(r, int(keyLen))
	if err != nil {
		return nil, err
	}
	kv.Val, err = common.ReadExactly(r, int(valLen))
	if err != nil {
		return nil, err
	}
	return &kv, nil
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
