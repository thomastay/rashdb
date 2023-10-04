package rashdb

import (
	"bytes"
	"os"
	"reflect"

	"github.com/thomastay/rash-db/pkg/app"
	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/disk"
	"github.com/thomastay/rash-db/pkg/varint"
	"github.com/vmihailenco/msgpack/v5"
)

type DB struct {
	path   string
	file   *os.File
	header disk.Header
	// lock   sync.Mutex
	table *tableNode
}

type DBOpenOptions struct {
	PageSize int
}

func Open(filename string, options *DBOpenOptions) (*DB, error) {
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
		if options.PageSize == 0 {
			db.header.PageSize = disk.DefaultDBPageSize
		} else {
			db.header.PageSize = uint16(options.PageSize)
		}
		return &db, nil
	}
	// Else, DB exists. Read from it.

	headerBytes, err := common.ReadExactly(db.file, disk.DBHeaderSize)
	if err != nil {
		return nil, err
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
	primaryKey string,
) error {
	tbl, err := db.createTable(tableName, tableType, primaryKey)
	if err != nil {
		return err
	}
	// TODO more tables
	db.table = tbl

	return nil
}

func (db *DB) Insert(
	tableName string,
	val interface{},
) error {
	if db.table.headers.Name != tableName {
		return ErrUnknownTableName
	}
	table := db.table

	// Iterate over the fields of the val struct, verifying that
	// 1. the primary key exists
	// 2. the column names are a subset of the known column names. The object shouldn't have any extra exported fields
	// It's a design choice here, but I choose to return an error if val contains extra fields, this helps identify bugs quickly
	// You could easily choose to silently ignore extra fields. Or even encode them as extra "slop" data. Honestly, that last one might be better,
	// since it allows for easy extensibility. I've definitely worked on a project where fields were just slapped onto the User struct without much thought

	v := reflect.ValueOf(val)
	typ := reflect.TypeOf(val)
	data := app.NewTableKeyValue()
	var foundPrimary bool

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldName := typ.Field(i).Name
		// feat: multi primary key
		if fieldName == table.headers.PrimaryKey[0].Key {
			data.Key[fieldName] = field.Interface()
			foundPrimary = true
			continue
		}

		if _, ok := table.columns[fieldName]; ok {
			fieldVal := field.Interface()
			// TODO check value
			data.Val[fieldName] = fieldVal
		} else {
			return ErrInsertInvalidKey(fieldName)
		}
	}
	if !foundPrimary {
		return ErrInsertNoPrimaryKey
	}
	// append
	table.data = append(table.data, data)

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
	tblBytes, err := db.table.MarshalBinary(int(db.header.PageSize))
	if err != nil {
		return err
	}
	buf.Write(tblBytes)
	db.file.Write(buf.Bytes())
	return nil
}

// Uses reflection to figure out what fields are available on a struct
func (db *DB) createTable(tableName string, tableType interface{}, primaryKey string) (*tableNode, error) {
	table := disk.Table{
		Name:       tableName,
		PrimaryKey: make([]disk.TableColumn, 1),
	}
	// feat: multi primary key
	table.PrimaryKey[0] = disk.TableColumn{
		Key:   primaryKey,
		Value: disk.DBStr,
	}

	cols := make([]disk.TableColumn, 0)
	colsMap := make(map[string]disk.DataType)
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

		// feat: multi primary key
		if col.Key != primaryKey {
			cols = append(cols, col)
			colsMap[col.Key] = col.Value
		}
	}
	table.Columns = cols
	return &tableNode{
		headers: table,
		columns: colsMap,
	}, nil
}

// Represents the table and its data
// This is an in-memory representation. On disk, the headers and data
// are stored in different locations
type tableNode struct {
	// --- Persisted to disk ---

	headers disk.Table
	// data not sorted, sort it lazily? Or maybe not?
	data []app.TableKeyValue

	// --- Not persisted to disk ---
	// this is the columns from the headers, but as a map.
	columns map[string]disk.DataType
}

func (n *tableNode) MarshalBinary(pageSize int) ([]byte, error) {
	var buf bytes.Buffer

	tblHeader := &n.headers
	enc := msgpack.NewEncoder(&buf)
	enc.UseArrayEncodedStructs(true)
	err := enc.Encode(tblHeader)
	if err != nil {
		return nil, err
	}
	// TODO sort data by primary key

	dataPage, err := n.EncodeDataAsPage(pageSize)
	if err != nil {
		return nil, err
	}
	pageBytes, err := dataPage.MarshalBinary(pageSize)
	if err != nil {
		return nil, err
	}
	buf.Write(pageBytes)
	return buf.Bytes(), nil
}

// Assumption: all data fits on a single page
func (n *tableNode) EncodeDataAsPage(pageSize int) (*disk.LeafPage, error) {
	page := disk.LeafPage{}
	numKV := len(n.data)
	if numKV > 65536 {
		panic("TODO: Multi-pages not implemented")
	}
	page.NumKV = uint16(numKV)

	cells := make([]disk.Cell, 2*numKV)
	for i, data := range n.data {
		// Marshal primary key and vals
		diskKV, err := app.EncodeKeyValue(&n.headers, &data)
		if err != nil {
			return nil, err
		}
		keyBytes, valBytes := diskKV.Key, diskKV.Val
		// TODO feat: overflow pages

		cells[i*2] = disk.Cell{
			Len:            varint.EncodeArrLen(len(keyBytes)),
			PayloadInitial: keyBytes,
		}
		cells[i*2+1] = disk.Cell{
			Len:            varint.EncodeArrLen(len(valBytes)),
			PayloadInitial: valBytes,
		}
	}
	page.Cells = cells

	// Calculate pointers
	offsets := make([]uint16, 2*numKV+1)
	ptr := 10 + 4*numKV
	// ^^ 8 bytes header, then 2 bytes each for (2n + 1) pointers
	offsets[0] = uint16(ptr)
	for i := 1; i < len(offsets); i++ {
		cell := cells[i-1]
		ptr += len(cell.Len) + len(cell.PayloadInitial)
		if cell.OffsetPageID != 0 {
			ptr += 4
		}
		// check for overflow
		if ptr >= pageSize {
			panic("TODO feat: multiple pages")
		}
		offsets[i] = uint16(ptr)
	}
	page.Pointers = offsets

	return &page, nil
}
