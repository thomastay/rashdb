package common

import (
	"bytes"
	"io"

	"github.com/thomastay/rash-db/pkg/varint"
)

func WriteVarIntToBuffer(buffer *bytes.Buffer, x int) error {
	buf, err := varint.Encode(x)
	if err != nil {
		return err
	}
	buffer.Write(buf)
	return nil
}

// Read exactly n bytes from the reader, or it returns an error
func ReadExactly(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	read, err := r.Read(buf)
	if err != nil {
		return nil, err
	}
	if read != n {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

// Write exactly n bytes to the writer, or it returns an error
func WriteExactly(w io.Writer, buf []byte) error {
	n, err := w.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return nil
}

// Panics on error. When you're 100% sure that an error will never happen.
func Check(err error) {
	if err != nil {
		panic(err)
	}
}
