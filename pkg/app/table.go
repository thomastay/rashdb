package app

// Represents a table's columns, so we know what data goes into them.
// These are encoded into arrays and serialized as messagepack objects for simplicity
type Table struct {
	Name       string
	PrimaryKey []TableColumn
	// Note: These columns don't contain the primary key(s)
	Columns []TableColumn
}

type TableColumn struct {
	Key   string
	Value DataType
}

//go:generate stringer -type=DataType
type DataType uint8

// Based on https://www.sqlite.org/datatype3.html
const (
	// Strings are likely to be the most common type, so they get 0
	DBStr DataType = iota
	DBInt
	DBReal
	DBNull
	DBText
	DBBlob
	DBJsonData
)
