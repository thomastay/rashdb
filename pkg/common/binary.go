package common

import "io"

func ReadUint16(r io.Reader) (uint16, error) {
	buf, err := ReadExactly(r, 2)
	if err != nil {
		return 0, err
	}
	var result uint16
	result = uint16(buf[0])*256 + uint16(buf[1])
	return result, nil
}

func CheckNoOverflow(x uint64) int {
	if x > uint64(maxU32) {
		panic("Overflow")
	}
	return int(x)
}

var (
	maxU32 = ^uint32(0)
)
