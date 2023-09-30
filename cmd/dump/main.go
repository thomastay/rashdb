package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/thomastay/rash-db/pkg/common"
	"github.com/thomastay/rash-db/pkg/disk"
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
	fmt.Println("========= Header ============")
	fmt.Printf("Magic: %s\n", string(header.Magic[:]))
	fmt.Printf("Version: %d\n", header.Version)
	fmt.Println("========= Table ============")
	table, err := parseTable(buf)
	if err != nil {
		return err
	}
	fmt.Printf("Table %s\n", table.Name)
	fmt.Printf("  Primary key %s\n", table.PrimaryKey)
	for i, col := range table.Columns {
		fmt.Printf("  Col %d: %s - %s\n", i, col.Key, col.Value.String())
	}

	kv, err := parseTableData(buf)
	if err != nil {
		return err
	}
	// TODO unmarshal these from messagepack? That should be a DB specific function
	fmt.Printf("Key: %s (%d)\nVal:%s (%d)\n", kv.Key, len(kv.Key), kv.Val, len(kv.Val))
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

func parseTableData(buf *bytes.Buffer) (*disk.KeyValue, error) {
	// TODO more than one elt please
	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, err
	}
	valLen, err := binary.ReadUvarint(buf)
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
	return &kv, nil
}
