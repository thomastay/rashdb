package disk

import (
	"encoding/binary"
)

// Represents a Leaf page
//
// ```
// (Header - fixed 8 bytes)
// +-----+
// + 0x1 + (Leaf)          (one byte)
// +-----+
// +--------------------+
// + Number of kv pairs +  (two bytes)
// +--------------------+
// +---------------------+
// + Offset of cell area + (two bytes)
// +---------------------+
// +----------+
// + Reserved +            (one byte)
// +----------+
//
// (Cell pointer area - all indexes are 2 bytes. There are 2n+1 pointers)
// +----------+----------+----------+----------+     +---------+
// + key1 Idx + val1 Idx + key2 Idx + val2 Idx + ... + End Idx +
// +----------+----------+----------+----------+     +---------+
//
// (Cell area - equals signs means variable length fields)
// +=======+=======+=======+=======+
// + Key 1 + Val 1 + Key 2 + Val 2 + ...
// +=======+=======+=======+=======+
//
// (Free space)
// ```
type LeafPage struct {
	// Header     byte  // Not actually stored in memory, but represented in the struct
	NumKV      uint16
	CellOffset uint16
	// reserved (one byte - not used for now)
	Pointers []uint16
	Cells    []Cell
}

func (p *LeafPage) MarshalBinary(pageSize int) ([]byte, error) {
	var err error
	buf := NewFixedBytesBuffer(make([]byte, pageSize))

	// ---- Write headers ---
	buf.WriteByte(HeaderLeafPage)
	err = binary.Write(buf, binary.BigEndian, p.NumKV)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, p.CellOffset)
	if err != nil {
		return nil, err
	}
	buf.WriteByte(0) // reserved for now
	// ---- End headers ---

	for _, ptr := range p.Pointers {
		err = binary.Write(buf, binary.BigEndian, ptr)
		if err != nil {
			return nil, err
		}
	}

	for _, cell := range p.Cells {
		buf.Write(cell.Len)
		buf.Write(cell.PayloadInitial)
		if cell.OffsetPageID != 0 {
			err = binary.Write(buf, binary.BigEndian, cell.OffsetPageID)
			if err != nil {
				return nil, err
			}
		}
	}

	result := buf.Bytes()
	if len(result) > pageSize {
		panic("Leaf page must fit onto page size, splitting should have happened earlier on.")
	}
	return buf.Bytes(), nil
}

// An opaque representation of anything, it could be a series of columns or just a single column
// The application layer is responsible for decoding this
type Cell struct {
	// Encoded as a varint, and represents the size of the entire payload, including overflow
	Len            []byte
	PayloadInitial []byte
	OffsetPageID   uint32 // if there is no offset, represented as 0 and not written to disk.
}

const (
	HeaderLeafPage = 0x1
)
