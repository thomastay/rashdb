package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/varint"
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
// +----------+
// + Reserved +            (five bytes)
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
	NumKV uint16
	// reserved (5 bytes - not used for now)
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
	// err = binary.Write(buf, binary.BigEndian, p.CellOffset)
	if err != nil {
		return nil, err
	}
	buf.Write(make([]byte, 5)) // reserved bytes
	// ---- End headers ---

	for _, ptr := range p.Pointers {
		err = binary.Write(buf, binary.BigEndian, ptr)
		if err != nil {
			return nil, err
		}
	}

	for _, cell := range p.Cells {
		buf.Write(varint.Encode64(cell.PayloadLen))
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

func Decode(pageBytes []byte, pageSize int) (*LeafPage, error) {
	if len(pageBytes) != pageSize {
		panic("Page size and page bytes don't match. This is an application level error")
	}
	pb := bytes.NewBuffer(pageBytes)

	pageType, err := pb.ReadByte()
	if err != nil {
		panic(err) // no way this can happen since we just checked the size above
	}
	if pageType != HeaderLeafPage {
		return nil, errors.New(fmt.Sprintf("Wrong header value %d", pageType))
		// TODO other types of pages?
	}
	p := LeafPage{}
	numKV16, err := common.ReadUint16(pb)
	if err != nil {
		return nil, err
	}
	p.NumKV = numKV16
	numKV := int(numKV16) // convenience

	p.Pointers = make([]uint16, 2*numKV+1)
	p.Cells = make([]Cell, 2*numKV)

	var prev uint16
	for i := 0; i < len(p.Pointers); i++ {
		ptr, err := common.ReadUint16(pb)
		if err != nil {
			return nil, err
		}
		if i > 0 && ptr < prev {
			// pointers can be the same as the previous, if the cell length is zero
			return nil, errors.New("Page corruption: Pointers should be non-decreasing")
		}
		prev = ptr
		p.Pointers[i] = ptr
	}

	for i := 0; i < len(p.Pointers)-1; i++ {
		curr, next := p.Pointers[i], p.Pointers[i+1]
		cellSize := int(next) - int(curr)
		if err != nil {
			return nil, err
		}
		cell := Cell{}

		payloadLen, err := varint.Decode(pb)
		if err != nil {
			return nil, err
		}
		cell.PayloadLen = payloadLen
		numBytesPayloadLen := varint.NumBytesNeededToEncode(payloadLen)
		// If there is no overflow, the payload len will be much larger than the cell size
		// Be careful! payloadLen could be MAX_INT64
		// Malicious actors / idiot programmer (aka me) could encode a really large payload len, we have to handle it properly
		hasOverflow := uint64(cellSize)-uint64(numBytesPayloadLen) < payloadLen
		if hasOverflow {
			panic("Overflow pages not implemented yet")
		}

		// Check for page corruption.
		// Don't cast to int here, which will silently truncate and cause all sorts of weird issues
		if payloadLen != uint64(cellSize)-uint64(numBytesPayloadLen) {
			return nil, errors.New("Page corruption: mismatch of pointer length and cell's own length")
		}

		// If there is no corruption and no overflow, payloadLen must fit within a 32 bit int. But let's check just to be safe.
		pLen := common.CheckNoOverflow(payloadLen)
		payload, err := common.ReadExactly(pb, pLen)
		if err != nil {
			return nil, err
		}
		cell.PayloadInitial = payload
		p.Cells[i] = cell
	}

	return &p, nil
}

// An opaque representation of anything, it could be a series of columns or just a single column
// The application layer is responsible for decoding this
type Cell struct {
	// Encoded as a varint, and represents the size of the entire payload, including overflow
	PayloadLen     uint64
	PayloadInitial []byte
	OffsetPageID   uint32 // if there is no offset, represented as 0 and not written to disk.
}

const (
	HeaderLeafPage = 0x1
)
