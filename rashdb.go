package rashdb

import (
	"bytes"
	"os"
	"reflect"

	"github.com/thomastay/rash-db/pkg/disk"
	"github.com/vmihailenco/msgpack/v5"
)

type DB struct {
	path   string
	file   *os.File
	header disk.Header
	// lock   sync.Mutex
	table *tableNode
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

	headerBytes := make([]byte, disk.DBHeaderSize)
	count, err := db.file.Read(headerBytes)
	if err != nil {
		return nil, err
	}
	if count != disk.DBHeaderSize {
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
	// TODO more tables
	db.table = tbl

	return nil
}

// Temp function until we do something better
func (db *DB) SyncAll() error {
	var buf bytes.Buffer
	headerBytes, err := db.header.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = buf.Write(headerBytes)
	if err != nil {
		return err
	}
	// TODO should probably write page by page
	tblBytes, err := db.table.MarshalBinary()
	if err != nil {
		return err
	}
	buf.Write(tblBytes)
	db.file.Write(buf.Bytes())
	return nil
}

// Uses reflection to figure out what fields are available on a struct
func (db *DB) createTable(tableName string, tableType interface{}) (*tableNode, error) {
	table := disk.Table{}
	table.Name = tableName
	cols := make([]disk.TableColumn, 0)
	defer func() { table.Columns = cols }()

	typ := reflect.TypeOf(tableType)
	for _, field := range reflect.VisibleFields(typ) {
		col := disk.TableColumn{Key: field.Name}

		switch field.Type.Kind() {
		case reflect.Bool,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			col.Value = disk.DBInt
		case reflect.Float32, reflect.Float64:
			col.Value = disk.DBReal
		case reflect.String:
			col.Value = disk.DBStr
		case reflect.Slice:
			elem := field.Type.Elem()
			switch elem.Kind() {
			case reflect.Uint8:
				col.Value = disk.DBBlob
			default:
				return nil, ErrInvalidTableValue
			}
		default:
			return nil, ErrInvalidTableValue
		}

		cols = append(cols, col)
	}
	table.Columns = cols
	return &tableNode{
		headers: table,
	}, nil
}

// Represents the data stored in a table.
// This is an in-memory representation
type tableNode struct {
	headers disk.Table
	vals []interface{}
}

func (n *tableNode) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	tblHeader := &n.headers
	enc := msgpack.NewEncoder(&buf)
	enc.UseArrayEncodedStructs(true)
	err := enc.Encode(tblHeader)
	if err != nil {
		return nil, err
	}

	// TODO write vals according to primary key
	for _, val := range n.vals {
		err = enc.Encode(val)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

