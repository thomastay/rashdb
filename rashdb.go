package rashdb

import (
	"bytes"
	"fmt"
	"os"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

type DB struct {
	path   string
	file   *os.File
	header dbHeader
	// lock   sync.Mutex
}

func Open(filename string) (*DB, error) {
	var err error
	db := DB{path: filename}
	db.file, err = os.Create(filename)
	if err != nil {
		return nil, err
	}
	// Check if the opened file exists
	info, err := db.file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		// initialize DB
		return &db, nil
	}
	// Else, DB exists. Read from it.

	headerBytes := make([]byte, 100)
	count, err := db.file.Read(headerBytes)
	if err != nil {
		return nil, err
	}
	if count != 100 {
		return nil, ErrInvalid
	}
	err = db.header.UnmarshalBinary(headerBytes)
	if err != nil {
		return nil, err
	}

	return &db, nil
}

func (db *DB) CreateTable(
	tableName string,
	tableType interface{},
) error {
	tbl, err := db.createTable(tableName, tableType)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseArrayEncodedStructs(true)
	err = enc.Encode(tbl)
	if err != nil {
		return err
	}
	fmt.Printf("%x\n", buf.Bytes())
	// TODO write to file

	return nil
}

// Uses reflection to figure out what fields are available on a struct
func (db *DB) createTable(tableName string, tableType interface{}) (*dbTable, error) {
	table := dbTable{}
	table.Name = tableName
	cols := make([]dbTableColumn, 0)
	defer func() { table.Columns = cols }()

	typ := reflect.TypeOf(tableType)
	for _, field := range reflect.VisibleFields(typ) {
		col := dbTableColumn{Key: field.Name}

		switch field.Type.Kind() {
		case reflect.Bool,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			col.Value = dbInt
		case reflect.Float32, reflect.Float64:
			col.Value = dbReal
		case reflect.String:
			col.Value = dbStr
		case reflect.Slice:
			elem := field.Type.Elem()
			switch elem.Kind() {
			case reflect.Uint8:
				col.Value = dbBlob
			default:
				return nil, ErrInvalidTableValue
			}
		default:
			return nil, ErrInvalidTableValue
		}

		cols = append(cols, col)
	}
	return &table, nil
}

// Represents a table's columns, so we know what data goes into them.
type dbTable struct {
	Name    string
	Columns []dbTableColumn
}

type dbTableColumn struct {
	Key   string
	Value dbValueType
}

//go:generate stringer -type=dbValueType
type dbValueType uint8

// Based on https://www.sqlite.org/datatype3.html
const (
	// Strings are likely to be the most common type, so they get 0
	dbStr dbValueType = iota
	dbInt
	dbReal
	dbNull
	dbText
	dbBlob
)
