package app

// Represents a table's columns, so we know what data goes into them.
// These are encoded into arrays and serialized as messagepack objects for simplicity
type TableMeta struct {
	Name       string
	PrimaryKey []TableColumn
	// Note: These columns don't contain the primary key(s)
	Columns []TableColumn
}

type TableColumn struct {
	Key   string
	Value DataType
}

// This is the "header" of the schema table
// The rows of the schema table are the metadata of the tables themselves.
// The schema table is always at page 1
// This schema is an implementation detail and should not be exposed to consumers
var schemaTable = TableMeta{
	Name: "rashdb_schema",
	PrimaryKey: []TableColumn{
		{"name", DBStr},
	},
	Columns: []TableColumn{
		{"root", DBInt}, // root page ID
		{"primary_key", DBJsonArr},
		{"columns", DBJsonArr},
	},
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
	DBJsonArr
)
