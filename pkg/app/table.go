package app

import (
	"github.com/thomastay/rash-db/pkg/disk"
	"github.com/vmihailenco/msgpack/v5"
)

// Represents a table's columns, so we know what data goes into them.
// These are encoded into arrays and serialized as messagepack objects for simplicity
type TableSchema struct {
	Name       string
	Root       int
	PrimaryKey []TableColumn
	// Note: These columns don't contain the primary key(s)
	Columns []TableColumn
}

func (m *TableSchema) EncodeAsSchemaRow() TableKeyValue {
	return TableKeyValue{
		Key: map[string]interface{}{
			"name": m.Name,
		},
		Val: map[string]interface{}{
			"primary_key": m.PrimaryKey,
			"columns":     m.Columns,
			"root":        m.Root,
		},
	}
}

type TableColumn struct {
	Key   string
	Value DataType
}

var _ msgpack.CustomEncoder = (*TableColumn)(nil)

func (c *TableColumn) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(2)
	err := enc.EncodeString(c.Key)
	if err != nil {
		return err
	}
	return enc.EncodeUint(uint64(c.Value))
}

var _ msgpack.CustomDecoder = (*TableColumn)(nil)

func (c *TableColumn) DecodeMsgpack(dec *msgpack.Decoder) error {
	k, err := dec.DecodeString()
	if err != nil {
		return err
	}
	c.Key = k
	v, err := dec.DecodeUint()
	if err != nil {
		return err
	}
	c.Value = DataType(v)
	return nil
}

// This is the "header" of the schema table
// The rows of the schema table are the schemas of the tables themselves.
// The schema table is always at page 1
// This schema is an implementation detail and should not be exposed to consumers
var schemaTable = TableSchema{
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

const DBSchemaPageID = 1

func NewSchemaPage(schemas []*TableSchema, pageSize int, pager *Pager, dbHeaders *disk.Header) *LeafNode {
	rows := make([]TableKeyValue, len(schemas))
	for i, schema := range schemas {
		rows[i] = schema.EncodeAsSchemaRow()
	}

	return &LeafNode{
		ID:        DBSchemaPageID,
		PageSize:  pageSize,
		Data:      rows,
		Headers:   &schemaTable,
		Pager:     pager,
		DBHeaders: dbHeaders,
	}
}

func DecodeSchemaPage(page *disk.LeafPage) ([]TableSchema, error) {
	kvs, err := DecodeKeyValuesOnPage(&schemaTable, page)
	if err != nil {
		return nil, err
	}
	tables := make([]TableSchema, len(kvs))
	for i, kv := range kvs {
		tables[i].Name = kv.Key["name"].(string)
		tables[i].Root = int(kv.Val["root"].(int64))
		tables[i].PrimaryKey = toTableColumns(kv.Val["primary_key"])
		tables[i].Columns = toTableColumns(kv.Val["columns"])
	}
	return tables, nil
}

func toTableColumns(encoded interface{}) []TableColumn {
	// It's encoded as an array of arrays
	arrInterface := encoded.([]interface{})
	result := make([]TableColumn, len(arrInterface))
	for i, i1 := range arrInterface {
		i2 := i1.([]interface{})
		if len(i2) != 2 {
			panic("Invalid TableColumn pair")
		}
		key := i2[0].(string)
		val := i2[1].(uint64)
		result[i].Key = key
		result[i].Value = DataType(val)
	}
	return result
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
