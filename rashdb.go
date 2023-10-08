package rashdb

import (
	"os"
	"reflect"

	"github.com/thomastay/rash-db/pkg/app"
	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/disk"
)

type DB struct {
	path   string
	file   *os.File
	header disk.Header
	// lock   sync.Mutex

	// Cache of recently created / updated tables.
	tables map[string]*tableNode
	pager  *app.Pager
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

		db.init()
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

	db.init()
	return &db, nil
}

// This is to be called to setup in memory data structures,
// after either the headers have been read from disk, or created.
func (db *DB) init() {
	db.pager = app.NewPager(int(db.header.PageSize), db.file)
	db.tables = make(map[string]*tableNode)
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
	db.tables[tableName] = tbl
	return nil
}

// Looks the table up from table cache or disk. Returns nil if it cannot find it.
func (db *DB) lookupTable(
	tableName string,
) (*tableNode, error) {
	if tbl, ok := db.tables[tableName]; ok {
		return tbl, nil
	}
	// if not, find it from the on-disk schema table
	// this must be an already created database
	pagerInfo, err := db.pager.Request(app.DBSchemaPageID)
	if err != nil {
		return nil, err
	}
	defer pagerInfo.Done()
	schemas, err := app.DecodeSchemaPage(pagerInfo.Page)
	if err != nil {
		return nil, err
	}
	for _, schema := range schemas {
		schema := schema
		// TODO only find the data that you need. This is too much data
		// The app layer needs to implement a function to find a value by key and return it
		// Then the app layer can pass a functor to the
		if schema.Name != tableName {
			continue
		}
		tblNode := tableNode{
			db:     db,
			schema: &schema,
		}
		// generate the columns array
		colsMap := make(map[string]app.DataType)
		for _, col := range schema.Columns {
			colsMap[col.Key] = col.Value
		}
		tblNode.columns = colsMap
		// deserialize data root page
		rootPage, err := db.pager.Request(schema.Root)
		if err != nil {
			return nil, err
		}
		defer rootPage.Done()
		tblNode.root = &app.LeafNode{
			ID:       schema.Root,
			PageSize: int(db.header.PageSize),
			Data:     make([]app.TableKeyValue, 0),
			Headers:  &schema,
			Pager:    db.pager,
		}
		return &tblNode, nil
	}
	return nil, nil
}

func (db *DB) Insert(
	tableName string,
	val interface{},
) error {
	table, err := db.lookupTable(tableName)
	if table == nil {
		return ErrUnknownTableName
	}
	if err != nil {
		return err
	}

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
		if fieldName == table.schema.PrimaryKey[0].Key {
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
	table.root.Data = append(table.root.Data, data)

	return nil
}

// Temp function until we do something better
func (db *DB) SyncAll() error {
	pagerInfo, err := db.tables["Bars"].root.EncodeDataAsPage()
	if err != nil {
		return err
	}
	err = db.pager.WritePage(pagerInfo)
	if err != nil {
		return err
	}

	tablePagerInfo, err := db.tables["Bars"].MarshalSchemaAsPage()
	if err != nil {
		return err
	}
	err = db.pager.WritePage(tablePagerInfo)
	if err != nil {
		return err
	}

	return db.file.Sync()
}

// Uses reflection to figure out what fields are available on a struct
func (db *DB) createTable(tableName string, tableType interface{}, primaryKey string) (*tableNode, error) {
	schema := app.TableSchema{
		Name:       tableName,
		PrimaryKey: make([]app.TableColumn, 1),
		Root:       db.pager.NextFreePageID(),
	}
	// feat: multi primary key
	schema.PrimaryKey[0] = app.TableColumn{
		Key:   primaryKey,
		Value: app.DBStr,
	}

	cols := make([]app.TableColumn, 0)
	colsMap := make(map[string]app.DataType)

	typ := reflect.TypeOf(tableType)
	for _, field := range reflect.VisibleFields(typ) {
		col := app.TableColumn{Key: field.Name}

		switch field.Type.Kind() {
		case reflect.Bool,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			col.Value = app.DBInt
		case reflect.Float32, reflect.Float64:
			col.Value = app.DBReal
		case reflect.String:
			col.Value = app.DBStr
		case reflect.Slice:
			elem := field.Type.Elem()
			switch elem.Kind() {
			case reflect.Uint8:
				col.Value = app.DBBlob
			default:
				col.Value = app.DBJsonArr
			}
		case reflect.Map:
			col.Value = app.DBJsonData
		default:
			return nil, ErrInvalidTableValue
		}

		// feat: multi primary key
		if col.Key != primaryKey {
			cols = append(cols, col)
			colsMap[col.Key] = col.Value
		}
	}
	schema.Columns = cols
	return &tableNode{
		db:      db,
		schema:  &schema,
		columns: colsMap,
		root: &app.LeafNode{
			ID:       schema.Root,
			PageSize: int(db.header.PageSize),
			Headers:  &schema,
			Pager:    db.pager,
		},
	}, nil
}

// Represents the table and its data
// This is an in-memory representation. On disk, the headers and data
// are stored in different locations
type tableNode struct {
	db      *DB
	schema  *app.TableSchema
	root    *app.LeafNode
	columns map[string]app.DataType
}

func (n *tableNode) MarshalSchemaAsPage() (app.PagerInfo, error) {
	schemaNode := app.NewSchemaPage(n.schema, int(n.db.header.PageSize), n.db.pager, &n.db.header)
	return schemaNode.EncodeDataAsPage()
}
