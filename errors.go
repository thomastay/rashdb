package rashdb

import "errors"

var (
	ErrInvalid           = errors.New("invalid database")
	ErrInvalidTableValue = errors.New("invalid value for table")
)
