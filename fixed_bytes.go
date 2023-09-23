package rashdb

import "errors"

// A fixed Bytes Buffer is a fixed, preallocated buffer of bytes that cannot grow
type fixedBytesBuffer struct {
	buf []byte
	pos int
}

func (buffer *fixedBytesBuffer) Cap() int {
	return len(buffer.buf) - buffer.pos
}

// Relinquish the inner buffer
func (buffer *fixedBytesBuffer) Bytes() []byte {
	return buffer.buf
}

// Implement io.Writer
// Best effort copy
func (buffer *fixedBytesBuffer) Write(bs []byte) (int, error) {
	numToCopy := min(len(bs), buffer.Cap())
	if numToCopy == len(bs) {
		// fast path this
		copy(buffer.buf[buffer.pos:], bs)
		buffer.pos += numToCopy
		return numToCopy, nil
	}
	buffer.pos += numToCopy
	copy(buffer.buf[buffer.pos:], bs[:numToCopy])
	return numToCopy, errors.New("Out of capacity")
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
