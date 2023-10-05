package app

import "os"

type readerStartingAt struct {
	file   *os.File
	offset int64
}

func (r readerStartingAt) Read(buf []byte) (int, error) {
	return r.file.ReadAt(buf, int64(r.offset))
}
