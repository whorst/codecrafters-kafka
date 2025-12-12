package common

func ReadVarIntUnsigned(offset int, header []byte) (int, int) {
	if offset >= len(header) {
		return 0, 0
	}

	var headerSize byte
	mostSignificantBit := 1
	var total uint64 = 0
	var numberOfBytesRead int = 0
	var shift uint = 0

	for mostSignificantBit >= 1 {
		if offset >= len(header) {
			return 0, 0
		}
		headerSize = header[offset]
		// Most significant bit is a flag that checks if there's another byte to consume after this
		mostSignificantBit = int(headerSize & byte(0x80))
		// Extract the 7 value bits
		valueBits := uint64(headerSize & byte(0x7f))
		// Shift the value bits left by the appropriate amount and OR into total
		total |= valueBits << shift
		shift += 7
		offset += 1
		numberOfBytesRead += 1
	}
	return int(total), numberOfBytesRead
}

// ReadVarIntSigned reads a signed varint from the byte array starting at the given offset.
// Returns the decoded signed integer value and the number of bytes read.
// Signed varints use zigzag encoding:
//   - Even numbers: encoded = 2 * value (decode: value = encoded / 2)
//   - Odd numbers: encoded = 2 * |value| - 1 (decode: value = -(encoded + 1) / 2)
func ReadVarIntSigned(offset int, header []byte) (int, int) {
	// First read as unsigned varint
	encoded, bytesRead := ReadVarIntUnsigned(offset, header)
	if bytesRead == 0 {
		return 0, 0
	}

	// Apply zigzag decoding
	// If encoded is even: value = encoded / 2
	// If encoded is odd: value = -(encoded + 1) / 2
	// This can be simplified to: value = (encoded >> 1) ^ -(encoded & 1)
	decoded := (encoded >> 1) ^ -(encoded & 1)

	return decoded, bytesRead
}
