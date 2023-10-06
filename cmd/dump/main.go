package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/thomastay/rash-db/pkg/app"
	"github.com/thomastay/rash-db/pkg/disk"
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

	bufferedStdout := bufio.NewWriter(os.Stdout)
	defer bufferedStdout.Flush()
	out := NewStreamer(bufferedStdout)

	out.StreamObjOpen("")
	out.StreamObjOpen("Header")
	out.StreamKV("Magic", string(header.Magic[:15]))
	out.StreamKV("Version", header.Version)
	out.StreamKV("PageSize", header.PageSize)
	out.StreamObjClose(true)
	out.StreamArrOpen("Tables")
	out.StreamObjOpen("")
	pageSize := int(header.PageSize)

	table, err := parseTable(dat, pageSize)
	if err != nil {
		return err
	}
	out.StreamKV("Name", table.Name)
	// feat: multi primary key
	out.StreamKV("PrimaryKey", table.PrimaryKey[0].Key)
	out.StreamArrOpen("Cols")
	for _, col := range table.Columns {
		out.StreamObjOpen("")
		out.StreamKV(col.Key, col.Value.String())
		out.StreamObjClose(true)
	}
	out.StreamArrClose()

	kvs, err := parseTableData(dat, table, 2, pageSize)
	if err != nil {
		return err
	}

	out.StreamArrOpen("Data")
	for _, kv := range kvs {
		out.StreamObjOpen("")
		cols := kv.Cols()
		for k, v := range cols {
			out.StreamKV(k, v)
		}
		out.StreamObjClose(true)
	}
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

func parseTable(buf []byte, pageSize int) (*app.TableMeta, error) {
	page, err := disk.Decode(buf[:pageSize], pageSize, 1)
	if err != nil {
		return nil, err
	}
	tables, err := app.DecodeSchemaPage(page)
	if err != nil {
		return nil, err
	}
	return &tables[0], nil
}

func parseTableData(buf []byte, tbl *app.TableMeta, pageID int, pageSize int) ([]*app.TableKeyValue, error) {
	startOffset := (pageID - 1) * pageSize

	pageBytes := buf[startOffset : startOffset+pageSize]
	page, err := disk.Decode(pageBytes, pageSize, pageID)
	if err != nil {
		return nil, err
	}
	return app.DecodeKeyValuesOnPage(tbl, page)
}
