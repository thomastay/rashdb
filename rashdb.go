package rashdb

import (
	"os"
)

type DB struct {
	path string
	file *os.File
}

func Open(filename string) (*DB, error) {

	return nil, nil
}
