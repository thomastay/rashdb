// Package app contains in-memory datastructures that are used by the the application layer
// of the database. These are not persisted to disk.
package app

import (
	"bytes"
	"io"

	"github.com/thomastay/rash-db/pkg/disk"
	"github.com/vmihailenco/msgpack/v5"
)

func DecodeKeyValue(tbl *disk.Table, kv disk.KeyValue) (*TableKeyValue, error) {
	// TODO assume more than one primary key
	result := TableKeyValue{
		Key: make(map[string]interface{}),
		Val: make(map[string]interface{}),
	}
	var keyData interface{}
	err := msgpack.Unmarshal(kv.Key, &keyData)
	if err != nil {
		return nil, err
	}
	result.Key[string(tbl.PrimaryKey)] = keyData

	// Values
	cols := tbl.Columns
	valBuf := bytes.NewBuffer(kv.Val)
	decoder := msgpack.NewDecoder(valBuf)
	i := 1 // skip primary key
	for {
		valData, err := decoder.DecodeInterface()
		if err != nil {
			if err == io.EOF { // EOF is expected
				break
			}
			return nil, err
		}
		col := cols[i]
		result.Val[col.Key] = valData

		i++
	}
	if i != len(cols) {
		return nil, io.ErrUnexpectedEOF
	}

	return &result, nil
}

type TableKeyValue struct {
	Key map[string]interface{}
	Val map[string]interface{}
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
