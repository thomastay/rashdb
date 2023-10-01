// Encodes data as a variable length integer
// ## Goals:
//
// 1. Pack as much data into as little bytes as possible
// 2. First byte determines the length exactly, to improve IO efficiency
// 3. Memcmp two pointers determines size without decoding
//
// ## Encoding
// 0-240 (1 byte): just 0-240 as itself
// 241-248 (2 bytes): 240 + 256 \* (X-241) + A1 (max of 2287)
// 249 (3 bytes): A1..A2 as big endian integer (2288 - 65535)
// 250 (4 bytes): A1..A3 as big-endian integer (2 ** 16 to 2**24-1)
// ...
// 255 (9 bytes): A1..A8 as a big endian integer. (2 ** 56 to 2 ** 64-1)
// 8 bytes can store vals of length 2^64-1, which is as much as a 64-bit machine can hold anyway.
package varint

import "errors"

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
		y := x - oneByteThreshold // We only need to encode the part that is bigger than the one byte threshold
		q, r := (y / 256), (y % 256)
		if q >= 8 {
			// sanity checks
			panic("q should be between 0-7")
		}
		b := make([]byte, 2)
		b[0] = 240 + byte(q)
		b[1] = byte(r)
		return b
	}

	// find threshold
	numTotalBytes := 9
	for i, threshold := range thresholds {
		if x <= threshold {
			numTotalBytes = i + 3
			break
		}
	}
	if numTotalBytes >= 10 {
		panic("wrong numTotalBytes")
	}

	// Store it as a big-endian integer
	b := make([]byte, numTotalBytes)
	b[0] = byte(249 + numTotalBytes - 3)
	i := numTotalBytes - 1
	for x > 0 {
		// Write to b backwards, so that the ultimate order is big-endian
		b[i] = byte(x)

		x = x >> 8
		i--
	}
	return b
}

const (
	oneByteThreshold  = 240
	twoBytesThreshold = 2287 // 240 + 256 * 7 + 255
	// These are encoded into an array to simplify implementation
	// threeBytesThreshold = 65535
	// fourBytesThreshold  = 16777215
	// fiveBytesThreshold  = 4294967295
	// sixBytesThreshold   = 1099511627775
	// sevenBytesThreshold = 281474976710655
	// eightBytesThreshold = 72057594037927935
)

var thresholds = []uint64{
	// The rest of these are just (2 ** (8*x) - 1)
	// 3 bytes, ... until 8 bytes
	65535,
	16777215,
	4294967295,
	1099511627775,
	281474976710655,
	72057594037927935,
}
