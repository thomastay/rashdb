package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

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
	fmt.Printf("%+v\n", header)
	table, err := parseTable(buf)
	fmt.Printf("%+v\n", table)
	if err != nil {
		return err
	}
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
