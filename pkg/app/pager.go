package app

import (
	"errors"
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
	currReqID uint64
}

func (p *Pager) Request(req PagerRequest) (PagerInfo, error) {
	if req.ID == 0 {
		return PagerInfo{}, errors.New("Pager: Page 0 is the null page")
	}
	startOffset := int64(req.ID) * int64(p.PageSize)

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
		ID:    req.ID,
		Page:  page,
		pager: p,
		reqID: p.currReqID,
	}
	if reqs, ok := p.inUse[req.ID]; ok {
		reqs[p.currReqID] = true
	} else {
		reqs := make(map[uint64]bool, 1)
		reqs[p.currReqID] = true
		p.inUse[req.ID] = reqs
	}
	p.currReqID++

	return result, nil
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

type PagerRequest struct {
	// The ID of the page requested
	ID int
}
