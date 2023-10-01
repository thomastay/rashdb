// Encodes data as a variable length integer
// ## Goals:
//
// 1. Pack as much data into as little bytes as possible
// 2. First byte determines the length exactly, to improve IO efficiency
// 3. Memcmp two pointers determines size without decoding
//
// ## Encoding
// 0-127 (1 byte): just 0-127 as itself
// 128-248 (2 bytes): 128 + 256 \* (X-128) + A1 (max of 31103)
// 249 (3 bytes): A1..A2 as big endian integer (31104 to 65535)
// 250 (4 bytes): A1..A3 as big-endian integer (2 ** 16 to 2**24-1)
// ...
// 255 (9 bytes): A1..A8 as a big endian integer. (2 ** 56 to 2 ** 64-1)
// 8 bytes can store vals of length 2^64-1, which is as much as a 64-bit machine can hold anyway.
package varint

import (
	"errors"
	"fmt"
	"io"
)

// This is a convenience method to encode array lengths. Callers who need flexibility should use Encode64
func Encode(x int) ([]byte, error) {
	if x < 0 {
		return nil, errors.New("Only positive values can be encoded")
	}
	return Encode64(uint64(x)), nil
}

func Encode64(x uint64) []byte {
	if x <= oneByteThreshold {
		return []byte{byte(x)}
	}
	if x <= twoBytesThreshold {
		y := x - twoByteDecodeRangeLowEnd // We only need to encode the part that is bigger than the one byte threshold
		q, r := (y / 256), (y % 256)
		if q > twoByteDecodeRangeLen {
			// sanity checks
			panic(fmt.Sprintf("q should be between 0 and 120, got %d", q))
		}
		b := make([]byte, 2)
		b[0] = twoByteDecodeRangeLowEnd + byte(q)
		b[1] = byte(r)
		return b
	}

	// find threshold
	numTotalBytes := maxVarIntLen64
	for i, threshold := range thresholds {
		if x <= threshold {
			numTotalBytes = i + 1
			break
		}
	}
	if numTotalBytes > maxVarIntLen64 {
		panic("wrong numTotalBytes")
	}

	// Store it as a big-endian integer
	b := make([]byte, numTotalBytes)
	b[0] = byte(multiByteDecodeRangeLowEnd + numTotalBytes - 3) // minus 3, because 249 maps to 3
	i := numTotalBytes - 1
	for x > 0 {
		// Write to b backwards, so that the ultimate order is big-endian
		b[i] = byte(x)

		x = x >> 8
		i--
	}
	return b
}

// Decode64 reads an encoded unsigned integer from r and returns it as a uint64.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes,
// ReadUvarint returns io.ErrUnexpectedEOF.
func Decode64(r io.ByteReader) (uint64, error) {
	var x uint64
	first, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if first <= oneByteThreshold {
		x = uint64(first)
		return x, nil
	}
	if first <= twoByteDecodeRangeHiEnd {
		second, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return x, io.ErrUnexpectedEOF
			}
			return x, err
		}
		q := uint64(first - twoByteDecodeRangeLowEnd)
		x = twoByteDecodeRangeLowEnd + 256*q + uint64(second)
		return x, nil
	}
	// Else, it is encoded as a big-endian integer in the rest of the bytes
	numBytesToRead := first - 247 // 249:2, 250:3, ... 255:8
	for i := 0; i < int(numBytesToRead); i++ {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return x, io.ErrUnexpectedEOF
			}
			return x, err
		}

		x <<= 8
		x += uint64(b)
	}
	return x, nil
}

const (
	maxVarIntLen64 = 9

	twoByteDecodeRangeLowEnd   = 128 // 240+1
	twoByteDecodeRangeHiEnd    = 248
	twoByteDecodeRangeLen      = 120 // 128...248
	multiByteDecodeRangeLowEnd = 249

	oneByteThreshold  = 127
	twoBytesThreshold = 31103 // 128 + 256 * 120 + 255
	// These are encoded into an array to simplify implementation
	// threeBytesThreshold = 65535
	// fourBytesThreshold  = 16777215
	// fiveBytesThreshold  = 4294967295
	// sixBytesThreshold   = 1099511627775
	// sevenBytesThreshold = 281474976710655
	// eightBytesThreshold = 72057594037927935
)

var thresholds = []uint64{
	// These are just the regular thresholds
	127,
	31103,
	// The rest of these are just (2 ** (8*x) - 1)
	// 3 bytes, ... until 8 bytes
	65535,
	16777215,
	4294967295,
	1099511627775,
	281474976710655,
	72057594037927935,
}
