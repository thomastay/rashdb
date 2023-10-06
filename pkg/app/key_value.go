// Package app contains in-memory datastructures that are used by the the application layer
// of the database. These are not persisted to disk.
package app

import (
	"bytes"
	"fmt"
	"io"

	"github.com/thomastay/rash-db/pkg/disk"
	"github.com/vmihailenco/msgpack/v5"
)

func DecodeKeyValue(tbl *Table, kv *KeyValue) (*TableKeyValue, error) {
	result := TableKeyValue{
		Key: make(map[string]interface{}),
		Val: make(map[string]interface{}),
	}
	// feat: multi primary key
	var keyData interface{}
	err := msgpack.Unmarshal(kv.Key, &keyData)
	if err != nil {
		return nil, err
	}
	result.Key[tbl.PrimaryKey[0].Key] = keyData

	// Values
	cols := tbl.Columns
	valBuf := bytes.NewBuffer(kv.Val)
	decoder := msgpack.NewDecoder(valBuf)
	for i := 0; i < len(cols); i++ {
		valData, err := decoder.DecodeInterface()
		if err != nil {
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		col := cols[i]
		result.Val[col.Key] = valData
	}

	return &result, nil
}

func DecodeKeyValuesOnPage(tbl *Table, page *disk.LeafPage) ([]*TableKeyValue, error) {
	if page.NumCells%2 == 1 {
		return nil, fmt.Errorf("Page has odd number of cells, %d", page.NumCells)
	}
	kvs := make([]*TableKeyValue, page.NumCells/2)
	var key []byte
	var err error
	for i, cell := range page.Cells {
		if i%2 == 0 {
			// Key
			key = cell.PayloadInitial // TODO overflow page
		} else {
			// Val
			val := cell.PayloadInitial
			kv := KeyValue{
				Key: key,
				Val: val,
			}
			kvs[i/2], err = DecodeKeyValue(tbl, &kv)
			if err != nil {
				return nil, err
			}
		}
	}
	return kvs, nil
}

func EncodeKeyValue(tbl *Table, kv *TableKeyValue) (*KeyValue, error) {
	// Marshal primary key and vals
	keyBytes, err := colsMapToBytes(tbl.PrimaryKey, kv.Key)
	if err != nil {
		return nil, err
	}
	valBytes, err := colsMapToBytes(tbl.Columns, kv.Val)
	if err != nil {
		return nil, err
	}
	return &KeyValue{
		Key: keyBytes,
		Val: valBytes,
	}, nil
}

type KeyValue struct {
	// Keys and values are stored as opaque structs and decoded as needed
	Key []byte
	Val []byte
}

func colsMapToBytes(
	columnOrder []TableColumn,
	cols map[string]interface{},
) ([]byte, error) {
	var buf bytes.Buffer
	// TODO use the msgpack pool to speed things up
	enc := msgpack.NewEncoder(&buf)
	for _, colType := range columnOrder {
		name := colType.Key
		if val, ok := cols[name]; ok {
			err := enc.Encode(val)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("Column %s not found in database", name)
		}
	}
	return buf.Bytes(), nil
}

type TableKeyValue struct {
	Key map[string]interface{}
	Val map[string]interface{}
}

func NewTableKeyValue() TableKeyValue {
	return TableKeyValue{
		Key: make(map[string]interface{}),
		Val: make(map[string]interface{}),
	}
}

// Just the columns of both the key and value (useful at the application layer)
func (kv *TableKeyValue) Cols() map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range kv.Key {
		res[k] = v
	}
	for k, v := range kv.Val {
		res[k] = v
	}
	return res
}
