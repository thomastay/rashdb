package rashdb

import (
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
	tblBytes, err := msgpack.Marshal(tbl)
	if err != nil {
		return err
	}
	fmt.Println(tblBytes)
	// TODO write to file

	return nil
}

// Uses reflection to figure out what fields are available on a struct
func (db *DB) createTable(tableName string, tableType interface{}) (*dbTable, error) {
	table := dbTable{}
	table.Name = tableName
	cols := make([]dbTableColumn, 0)
	table.Columns = cols

	typ := reflect.TypeOf(tableType)
	for _, field := range reflect.VisibleFields(typ) {
		col := dbTableColumn{Key: field.Name}
		cols = append(cols, col)

		switch field.Type.Kind() {
		case reflect.Bool, reflect.Uint8:
			col.Value = dbValueU8
		case reflect.Uint, reflect.Uint16, reflect.Uint32:
			col.Value = dbValueU32
		case reflect.Uint64:
			col.Value = dbValueU64
		case reflect.Int8:
			col.Value = dbValueI8
		case reflect.Int, reflect.Int16, reflect.Int32:
			col.Value = dbValueI32
		case reflect.Int64:
			col.Value = dbValueI64
		case reflect.Float32:
			col.Value = dbValueF32
		case reflect.Float64:
			col.Value = dbValueF64
		case reflect.String:
			col.Value = dbValueStr
		default:
			return nil, ErrInvalidTableValue
		}
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
type dbValueType uint32

const (
	// Strings are likely to be the most common type, so they get 0
	dbValueStr dbValueType = iota
	dbValueF32
	dbValueF64
	dbValueI8
	dbValueI32
	dbValueI64
	dbValueU8
	dbValueU32
	dbValueU64
)
