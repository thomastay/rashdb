package rashdb

import (
	"errors"
	"fmt"
)

var (
	ErrInvalid            = errors.New("invalid database")
	ErrInvalidTableValue  = errors.New("invalid value for table")
	ErrUnknownTableName   = errors.New("unknown table name")
	ErrInsertNoPrimaryKey = errors.New("insert: no primary key")
)

func ErrInsertInvalidKey(name string) error {
	return fmt.Errorf("insert: invalid key name %s", name)
}
