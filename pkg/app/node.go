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
	// TODO determine - is data sorted on insert? or only on commits?
	Data    []TableKeyValue
	Headers *TableMeta

	Pager *Pager
}

// Assumption: all data fits on a single page
func (n *LeafNode) EncodeDataAsPage() (PagerInfo, error) {
	page := disk.LeafPage{}
	// The number of keys + number of values
	numCells := 2 * len(n.Data)
	if numCells > 65536 {
		panic("TODO: Multi-pages not implemented")
	}
	page.NumCells = uint16(numCells)

	cells := make([]disk.Cell, numCells)
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
	offsets := make([]uint16, numCells)
	ptr := 8 + 2*numCells
	// ^^ 8 bytes header, then 2 bytes each for n pointers
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

	newFreeLeafPage := n.Pager.NewFreeLeafPage()
	newFreeLeafPage.Page = &page
	return newFreeLeafPage, nil
}
