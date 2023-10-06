package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/varint"
)

// Represents a Leaf page
//
// ```
// (Header - fixed 8 bytes)
// +-----+
// + 0x1 + (Leaf)        		(one byte)
// +-----+
// +---------------------+
// + Number of cells (n) +  (two bytes)
// +---------------------+
// +----------+
// + Reserved +          		(five bytes)
// +----------+
//
// (Cell pointer area - all indexes are 2 bytes. There are n pointers)
// (pointers point to the END of the cell. The start of the first cell can be determined from the number of cells)
// +----------+----------+----------+----------+
// + key1 Len + val1 Len + key2 Len + val2 Len + ...
// +----------+----------+----------+----------+
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
	NumCells uint16
	// reserved (5 bytes - not used for now)
	Pointers []uint16
	Cells    []Cell
}

func (p *LeafPage) MarshalBinary(pageSize int) ([]byte, error) {
	var err error
	buf := NewFixedBytesBuffer(make([]byte, pageSize))

	// ---- Write headers ---
	common.Check(buf.WriteByte(HeaderLeafPage))
	common.Check(binary.Write(buf, binary.BigEndian, p.NumCells))
	common.Check(common.WriteExactly(buf, make([]byte, pageHeaderReservedSize))) // reserved bytes
	// ---- End headers ---

	for _, ptr := range p.Pointers {
		err = binary.Write(buf, binary.BigEndian, ptr)
		if err != nil {
			return nil, err
		}
	}

	for _, cell := range p.Cells {
		err = common.WriteExactly(buf, varint.Encode64(cell.PayloadLen))
		if err != nil {
			return nil, err
		}
		err = common.WriteExactly(buf, cell.PayloadInitial)
		if err != nil {
			return nil, err
		}
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
	common.Check(err)

	if pageType != HeaderLeafPage {
		return nil, fmt.Errorf("Wrong header value %d", pageType)
		// TODO other types of pages?
	}
	p := LeafPage{}
	noofCells16, err := common.ReadUint16(pb)
	if err != nil {
		return nil, err
	}
	if noofCells16 > maxNumCellsPerPage(pageSize) {
		return nil, errPageCorruption("too many kvs", int(maxNumCellsPerPage(pageSize)), uint64(noofCells16))
	}
	p.NumCells = noofCells16
	numCells := int(noofCells16) // convenience

	_, err = pb.Read(make([]byte, pageHeaderReservedSize))
	common.Check(err)
	// ---- End reading header ----

	p.Pointers = make([]uint16, numCells)
	p.Cells = make([]Cell, numCells)

	var prev uint16
	for i := 0; i < len(p.Pointers); i++ {
		ptr, err := common.ReadUint16(pb)
		if err != nil {
			return nil, err
		}
		if i > 0 && ptr < prev {
			// pointers can be the same as the previous, if the cell length is zero
			return nil, errPageCorruption("Pointers should be non-decreasing", int(prev), uint64(ptr))
		}
		if int(ptr) >= pageSize {
			return nil, errPageCorruption("Pointers should be within the page size", pageSize, uint64(ptr))
		}
		prev = ptr
		p.Pointers[i] = ptr
	}

	for i := 0; i < len(p.Pointers); i++ {
		var cellSize int
		if i == 0 {
			prev := pageHeaderSize + 2*int(p.NumCells)
			cellSize = int(p.Pointers[i]) - prev
		} else {
			prev, curr := p.Pointers[i-1], p.Pointers[i]
			cellSize = int(curr) - int(prev)
		}
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
			return nil, errPageCorruption("mismatch of pointer length and cell's own length", cellSize-numBytesPayloadLen, payloadLen)
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
	HeaderLeafPage         = 0x1
	pageHeaderSize         = 8
	pageHeaderReservedSize = 5
)

func maxNumCellsPerPage(pageSize int) uint16 {
	// Keys must always have at least 1 byte payload. Values can have zero.
	// 1 byte payload requires 1 byte varint. So each kv pair must take up at least 2 bytes.
	// Each pointer to a kv pair takes up 4 bytes. So we have the equation
	// 		pageSize <= 8 + 4n + 2n
	// => n <= (pageSize - 8) / 6
	return uint16(math.Floor(float64(pageSize-pageHeaderSize) / float64(6)))
}

func errPageCorruption(s string, expected int, got uint64) error {
	return fmt.Errorf("Page corruption: %s, expected %d, got %d", s, expected, got)
}
