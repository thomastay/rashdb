package app

import (
	"github.com/thomastay/rash-db/pkg/disk"
	"github.com/thomastay/rash-db/pkg/varint"
)

// A leaf node represents a leaf page, deserialized into memory
// When you insert/delete, you act on a leaf page, which then gets written to disk (possibly as multiple pages)
// and the end of the transaction
type LeafNode struct {
	ID       int
	PageSize int
	Data     []TableKeyValue
	// this is the columns from the headers, but as a map.
	Columns map[string]disk.DataType
	Headers *disk.Table
}

// Assumption: all data fits on a single page
func (n *LeafNode) EncodeDataAsPage() (PagerInfo, error) {
	page := disk.LeafPage{}
	numKV := len(n.Data)
	if numKV > 65536 {
		panic("TODO: Multi-pages not implemented")
	}
	page.NumKV = uint16(numKV)

	cells := make([]disk.Cell, 2*numKV)
	for i, data := range n.Data {
		// Marshal primary key and vals
		diskKV, err := EncodeKeyValue(n.Headers, &data)
		if err != nil {
			return PagerInfo{}, err
		}
		keyBytes, valBytes := diskKV.Key, diskKV.Val
		// TODO feat: overflow pages

		cells[i*2] = disk.Cell{
			PayloadLen:     uint64(len(keyBytes)),
			PayloadInitial: keyBytes,
		}
		cells[i*2+1] = disk.Cell{
			PayloadLen:     uint64(len(valBytes)),
			PayloadInitial: valBytes,
		}
	}
	page.Cells = cells

	// Calculate pointers
	offsets := make([]uint16, 2*numKV)
	ptr := 8 + 4*numKV
	// ^^ 8 bytes header, then 2 bytes each for 2n pointers
	for i := 0; i < len(offsets); i++ {
		cell := cells[i]
		ptr += varint.NumBytesNeededToEncode(cell.PayloadLen) + len(cell.PayloadInitial)
		if cell.OffsetPageID != 0 {
			ptr += 4
		}
		// check for overflow
		if ptr >= n.PageSize {
			panic("TODO feat: multiple pages")
		}
		offsets[i] = uint16(ptr)
	}
	page.Pointers = offsets

	return PagerInfo{ID: n.ID, Page: &page}, nil
}
