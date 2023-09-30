package disk

import (
	"bytes"
	"encoding/binary"
	"io"
)

func WriteUVarIntToBuffer(buffer *bytes.Buffer, x uint64) {
	// Weird that this is not in the standard library?
	var buf []byte
	buf = binary.AppendUvarint(buf, x)
	buffer.Write(buf)
}

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
