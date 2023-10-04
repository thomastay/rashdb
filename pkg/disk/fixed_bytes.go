package disk

import "errors"

var ErrOutOfCapacity = errors.New("out of capacity")

// A fixed Bytes Buffer is a fixed, preallocated buffer of bytes that cannot grow
type fixedBytesBuffer struct {
	buf []byte
	pos int
}

func NewFixedBytesBuffer(buffer []byte) *fixedBytesBuffer {
	return &fixedBytesBuffer{buf: buffer}
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
	return numToCopy, ErrOutOfCapacity
}

func (buffer *fixedBytesBuffer) WriteByte(c byte) error {
	if buffer.Cap() == 0 {
		return ErrOutOfCapacity
	}
	buffer.buf[buffer.pos] = c
	buffer.pos++
	return nil
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
