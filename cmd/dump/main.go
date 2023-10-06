package main

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/thomastay/rash-db/pkg/app"
	"github.com/thomastay/rash-db/pkg/common"
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
	filename := os.Args[1]
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	headerBytes, err := common.ReadExactly(file, disk.DBHeaderSize)
	if err != nil {
		return err
	}
	header, err := parseHeader(headerBytes)
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

	table, err := parseTable(file, pageSize)
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

	kvs, err := parseTableData(file, table, 2, pageSize)
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

func parseHeader(buf []byte) (disk.Header, error) {
	if len(buf) != disk.DBHeaderSize {
		return disk.Header{}, errors.New("too small header")
	}
	var header disk.Header
	header.UnmarshalBinary(buf)
	return header, nil
}

func parseTable(file *os.File, pageSize int) (*app.TableMeta, error) {
	file.Seek(0, io.SeekStart)
	buf, err := common.ReadExactly(file, pageSize)
	if err != nil {
		return nil, err
	}
	page, err := disk.Decode(buf, pageSize, 1)
	if err != nil {
		return nil, err
	}
	tables, err := app.DecodeSchemaPage(page)
	if err != nil {
		return nil, err
	}
	return &tables[0], nil
}

func parseTableData(file *os.File, tbl *app.TableMeta, pageID int, pageSize int) ([]*app.TableKeyValue, error) {
	startOffset := (pageID - 1) * pageSize

	buf := make([]byte, pageSize)
	n, err := file.ReadAt(buf, int64(startOffset))
	if n != pageSize {
		return nil, io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, err
	}
	page, err := disk.Decode(buf, pageSize, pageID)
	if err != nil {
		return nil, err
	}
	return app.DecodeKeyValuesOnPage(tbl, page)
}
