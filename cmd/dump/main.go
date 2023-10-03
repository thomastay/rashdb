package main

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/thomastay/rash-db/pkg/app"
	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/disk"
	"github.com/thomastay/rash-db/pkg/varint"
	"github.com/vmihailenco/msgpack/v5"
)

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	// Dump the contents of the DB to the command line
	dat, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(dat)
	header, err := parseHeader(buf)
	if err != nil {
		return err
	}

	out := NewStreamer(os.Stdout)
	defer out.Flush()

	out.StreamObjOpen("")
	out.StreamObjOpen("Header")
	out.StreamKV("Magic", string(header.Magic[:]))
	out.StreamKV("Version", header.Version)
	out.StreamObjClose(true)
	out.StreamArrOpen("Tables")
	out.StreamObjOpen("")
	table, err := parseTable(buf)
	if err != nil {
		return err
	}
	out.StreamKV("Name", table.Name)
	out.StreamKV("PrimaryKey", table.PrimaryKey)
	out.StreamArrOpen("Cols")
	for _, col := range table.Columns {
		out.StreamObjOpen("")
		out.StreamKV(col.Key, col.Value.String())
		out.StreamObjClose(true)
	}
	out.StreamArrClose()

	kv, err := parseTableData(buf, &table)
	if err != nil {
		return err
	}

	out.StreamArrOpen("Data")
	// TODO for data in data
	out.StreamObjOpen("")
	cols := kv.Cols()
	for k, v := range cols {
		out.StreamKV(k, v)
	}
	out.StreamObjClose(true)
	out.StreamArrClose()      // end data
	out.StreamObjClose(true)  // end table
	out.StreamArrClose()      // end tables
	out.StreamObjClose(false) // end
	return nil
}

func parseHeader(buf io.Reader) (disk.Header, error) {
	headerBuf := make([]byte, disk.DBHeaderSize)
	n, err := buf.Read(headerBuf)
	if err != nil {
		return disk.Header{}, err
	}
	if n != disk.DBHeaderSize {
		return disk.Header{}, errors.New("too small header")
	}
	var header disk.Header
	header.UnmarshalBinary(headerBuf)
	return header, nil
}

func parseTable(buf io.Reader) (disk.Table, error) {
	var tbl disk.Table
	dec := msgpack.NewDecoder(buf)
	err := dec.Decode(&tbl)
	if err != nil {
		return disk.Table{}, err
	}
	return tbl, nil
}

func parseTableData(buf *bytes.Buffer, tbl *disk.Table) (*app.TableKeyValue, error) {
	// TODO more than one elt please
	keyLen, err := varint.Decode(buf)
	if err != nil {
		return nil, err
	}
	valLen, err := varint.Decode(buf)
	if err != nil {
		return nil, err
	}
	kv := disk.KeyValue{}
	kv.Key, err = common.ReadExactly(buf, int(keyLen))
	if err != nil {
		return nil, err
	}
	kv.Val, err = common.ReadExactly(buf, int(valLen))
	if err != nil {
		return nil, err
	}
	appKV, err := app.DecodeKeyValue(tbl, kv)
	if err != nil {
		return nil, err
	}

	return appKV, nil
}
