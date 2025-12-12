package common

import "encoding/binary"

// intToVarInt converts an integer to varint-encoded bytes.
// Varints use 7 bits per byte for the value, with the MSB (0x80) indicating
// whether there are more bytes to follow.
func intToVarInt(value int) []byte {
	return uint64ToVarInt(uint64(value))
}

// uint64ToVarInt converts a uint64 to varint-encoded bytes
func uint64ToVarInt(value uint64) []byte {
	if value == 0 {
		return []byte{0x00}
	}

	var result []byte
	for value > 0 {
		// Extract the lower 7 bits
		byteValue := byte(value & 0x7f)
		value >>= 7

		// If there are more bytes to follow, set the MSB
		if value > 0 {
			byteValue |= 0x80
		}

		result = append(result, byteValue)
	}

	// Reverse the result because varints are encoded with most significant byte first
	// (readVarInt reads the first byte and shifts left, so first byte is most significant)
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// bytesToVarInt converts an array of bytes (representing an integer) to varint-encoded bytes.
// The input bytes are interpreted as a big-endian integer.
func BytesToVarInt(data []byte) []byte {
	// Convert bytes to integer first
	intValue := BytesToInt(data)
	// Then convert to varint
	return intToVarInt(intValue)
}

func BytesToInt(dataBytes []byte) int {
	var retVal int
	if len(dataBytes) > 10 {
		panic("Input too Large")
	}
	if len(dataBytes) == 1 {
		retVal = int(dataBytes[0])
	}
	if len(dataBytes) > 1 && len(dataBytes) < 5 {
		for idx := range dataBytes {
			retVal = retVal << 8
			retVal |= int(dataBytes[idx])
		}
	}
	return retVal
}

func IntToFourBytes(value int) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, uint32(value))
	return result
}
