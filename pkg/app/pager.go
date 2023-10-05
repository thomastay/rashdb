package app

import (
	"errors"
	"io"
	"os"

	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/disk"
)

// A pager is a service that coordinates fetching and writing pages to disk
// It is also responsible to maintaining the list of pages still accessed by all threads,
// So we don't delete data from disk before we are able to read it
type Pager struct {
	// mu sync.Mutex
	PageSize int
	file     *os.File

	inUse map[int]map[uint64]bool // list of pages in use
	// An counter that increments with every request
	// Don't use zero here! zero is a null value
	currReqID      uint64
	nextFreePageID int // points to one past the last page
}

func NewPager(pageSize int, file *os.File) *Pager {
	return &Pager{
		PageSize:       pageSize,
		file:           file,
		inUse:          make(map[int]map[uint64]bool),
		currReqID:      1,
		nextFreePageID: 2, // 1 is always in use, as the root page
	}
}

func (p *Pager) Request(ID int) (PagerInfo, error) {
	if ID == 0 {
		return PagerInfo{}, errZeroPage
	}
	startOffset := p.pageStart(ID)
	wrappedReader := readerStartingAt{p.file, startOffset}
	bytes, err := common.ReadExactly(wrappedReader, p.PageSize)
	if err != nil {
		return PagerInfo{}, err
	}
	page, err := disk.Decode(bytes, p.PageSize)
	if err != nil {
		return PagerInfo{}, err
	}

	result := PagerInfo{
		ID:    ID,
		Page:  page,
		pager: p,
		reqID: p.currReqID,
	}
	if reqs, ok := p.inUse[ID]; ok {
		reqs[p.currReqID] = true
	} else {
		reqs := make(map[uint64]bool, 1)
		reqs[p.currReqID] = true
		p.inUse[ID] = reqs
	}
	p.currReqID++

	return result, nil
}

func (p *Pager) WritePage(info PagerInfo) error {
	// Check some basic details
	if info.ID == 0 {
		return errZeroPage
	}
	if info.Page == nil {
		return errors.New("Invalid pager write request")
	}

	// Write page to disk! Lets go
	startOffset := p.pageStart(info.ID)
	pageBytes, err := info.Page.MarshalBinary(p.PageSize)
	if err != nil {
		return err
	}
	written, err := p.file.WriteAt(pageBytes, startOffset)
	if err != nil {
		return err
	}
	if written != p.PageSize {
		// TODO: rollback? How to recover here?
		return io.ErrShortWrite
	}
	if info.reqID != 0 {
		// This is an existing pagerInfo that came from a read request.
		// Mark it as read
		info.Done()
	}
	return nil
}

func (p *Pager) NewFreeLeafPage() PagerInfo {
	result := PagerInfo{
		ID: p.nextFreePageID,
	}
	p.nextFreePageID++
	return result
}

func (p *Pager) pageStart(ID int) int64 {
	if ID == 0 {
		panic("non zero pages. Check in caller function")
	}
	return int64(ID-1) * int64(p.PageSize)
}

type PagerInfo struct {
	// the Page ID
	ID   int
	Page *disk.LeafPage

	pager *Pager
	reqID uint64
}

func (info *PagerInfo) Done() {
	delete(info.pager.inUse[info.ID], info.reqID)
}

var errZeroPage = errors.New("Pager: Page 0 is the null page")
