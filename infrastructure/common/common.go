package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func ReadVarInt(offset int, header []byte) (int, int) {
	var headerSize byte
	mostSignificantBit := 1
	var total uint64 = 0
	var numberOfBytesRead int = 0

	for mostSignificantBit >= 1 {
		if err := binary.Read(bytes.NewReader(header[offset:]), binary.BigEndian, &headerSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return 0, 0
		}
		// Most significant bit is a flag that checks if there's another byte to consume after this, 1 means yes and 0 means no
		mostSignificantBit = int(headerSize & byte(0x80))
		// These are the value bits that we add to total. We accomplish this by shifting left total and doing an or operations
		// on the value bits
		valueBits := uint64(headerSize & byte(0x7f))
		total = total<<7 | valueBits
		offset += 1
		numberOfBytesRead += 1
	}
	return int(total), numberOfBytesRead
}
