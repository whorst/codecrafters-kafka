package common

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// Test_ReadVarIntUnsigned tests the unsigned varint decoding
func Test_ReadVarIntUnsigned(t *testing.T) {
	tests := []struct {
		name          string
		data          []byte
		offset        int
		expectedValue int
		expectedBytes int
		description   string
	}{
		{
			name:          "single byte value",
			data:          []byte{0x02},
			offset:        0,
			expectedValue: 2,
			expectedBytes: 1,
			description:   "Single byte should decode correctly",
		},
		{
			name:          "two byte value 144",
			data:          []byte{0x90, 0x01},
			offset:        0,
			expectedValue: 144,
			expectedBytes: 2,
			description:   "0x90 0x01 should decode to 144 (16 | (1 << 7))",
		},
		{
			name:          "two byte value 128",
			data:          []byte{0x80, 0x01},
			offset:        0,
			expectedValue: 128,
			expectedBytes: 2,
			description:   "0x80 0x01 should decode to 128 (0 | (1 << 7))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, bytesRead := ReadVarIntUnsigned(tt.offset, tt.data)

			if value != tt.expectedValue {
				t.Errorf("%s: expected value %d, got %d", tt.description, tt.expectedValue, value)
			}

			if bytesRead != tt.expectedBytes {
				t.Errorf("%s: expected %d bytes read, got %d", tt.description, tt.expectedBytes, bytesRead)
			}
		})
	}
}

func Test_ReadVarIntSignedTwo(t *testing.T) {
	tests := []struct {
		name          string
		data          []byte
		offset        int
		expectedValue int
		expectedBytes int
		description   string
	}{
		// Zero
		{
			name:          "zero",
			data:          []byte{0x00},
			offset:        0,
			expectedValue: 0,
			expectedBytes: 1,
			description:   "Zero should decode to 0",
		},
		// Test case for 0x90, 0x01 which should decode to 72 (signed)
		{
			name:          "positive 72 (two bytes)",
			data:          []byte{0x90, 0x01},
			offset:        0,
			expectedValue: 72,
			expectedBytes: 2,
			description:   "0x90 0x01 should decode to 72 (144 zigzag-decoded: 144/2 = 72)",
		},
		// Positive numbers (even encoded values)
		{
			name:          "positive one",
			data:          []byte{0x02},
			offset:        0,
			expectedValue: 1,
			expectedBytes: 1,
			description:   "Encoded 2 (2*1) should decode to 1",
		},
		{
			name:          "positive two",
			data:          []byte{0x04},
			offset:        0,
			expectedValue: 2,
			expectedBytes: 1,
			description:   "Encoded 4 (2*2) should decode to 2",
		},
		{
			name:          "positive three",
			data:          []byte{0x06},
			offset:        0,
			expectedValue: 3,
			expectedBytes: 1,
			description:   "Encoded 6 (2*3) should decode to 3",
		},
		// Negative numbers (odd encoded values)
		{
			name:          "negative one",
			data:          []byte{0x01},
			offset:        0,
			expectedValue: -1,
			expectedBytes: 1,
			description:   "Encoded 1 (2*|-1|-1) should decode to -1",
		},
		{
			name:          "negative two",
			data:          []byte{0x03},
			offset:        0,
			expectedValue: -2,
			expectedBytes: 1,
			description:   "Encoded 3 (2*|-2|-1) should decode to -2",
		},
		{
			name:          "negative three",
			data:          []byte{0x05},
			offset:        0,
			expectedValue: -3,
			expectedBytes: 1,
			description:   "Encoded 5 (2*|-3|-1) should decode to -3",
		},
		// Multi-byte varints - positive
		{
			name:          "positive 63 (single byte max for 7 bits)",
			data:          []byte{0x7E}, // 2 * 63 = 126 = 0x7E
			offset:        0,
			expectedValue: 63,
			expectedBytes: 1,
			description:   "Largest single-byte positive value",
		},
		{
			name:          "positive 64 (two bytes)",
			data:          []byte{0x80, 0x01}, // 2 * 64 = 128, varint: 0x80 0x01 (0 | (1 << 7) = 128)
			offset:        0,
			expectedValue: 64,
			expectedBytes: 2,
			description:   "Two-byte positive value",
		},
		// Multi-byte varints - negative
		// Note: -64 zigzag encodes to 127, which is 0x7F (single byte)
		// For a 2-byte negative, we need -65 which zigzag encodes to 129
		{
			name:          "negative 65 (two bytes)",
			data:          []byte{0x81, 0x01}, // 2 * 65 - 1 = 129, varint: 0x81 0x01
			offset:        0,
			expectedValue: -65,
			expectedBytes: 2,
			description:   "Two-byte negative value",
		},
		{
			name:          "negative 64 (single byte)",
			data:          []byte{0x7F}, // 2 * 64 - 1 = 127, varint: 0x7F
			offset:        0,
			expectedValue: -64,
			expectedBytes: 1,
			description:   "Single-byte negative value",
		},
		// Offset testing
		{
			name:          "with offset",
			data:          []byte{0xFF, 0x05, 0xFF},
			offset:        1,
			expectedValue: -3,
			expectedBytes: 1,
			description:   "Reading from non-zero offset",
		},
		// Edge cases - using dynamically generated encodings
		// These will be tested in the zigzag encoding test instead
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, bytesRead := ReadVarIntSigned(tt.offset, tt.data)

			if value != tt.expectedValue {
				t.Errorf("%s: expected value %d, got %d", tt.description, tt.expectedValue, value)
			}

			if bytesRead != tt.expectedBytes {
				t.Errorf("%s: expected %d bytes read, got %d", tt.description, tt.expectedBytes, bytesRead)
			}
		})
	}
}

func Test_ReadVarIntSigned_EdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		data          []byte
		offset        int
		expectedValue int
		expectedBytes int
	}{
		{
			name:          "empty data",
			data:          []byte{},
			offset:        0,
			expectedValue: 0,
			expectedBytes: 0,
		},
		{
			name:          "offset beyond data length",
			data:          []byte{0x02},
			offset:        10,
			expectedValue: 0,
			expectedBytes: 0,
		},
		{
			name:          "multiple values in sequence",
			data:          []byte{0x02, 0x01, 0x04},
			offset:        0,
			expectedValue: 1,
			expectedBytes: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, bytesRead := ReadVarIntSigned(tt.offset, tt.data)

			if value != tt.expectedValue {
				t.Errorf("expected value %d, got %d", tt.expectedValue, value)
			}

			if bytesRead != tt.expectedBytes {
				t.Errorf("expected %d bytes read, got %d", tt.expectedBytes, bytesRead)
			}
		})
	}
}

// Test_ReadVarIntSigned_ZigzagEncoding verifies the zigzag encoding/decoding round-trip
func Test_ReadVarIntSigned_ZigzagEncoding(t *testing.T) {
	// Test the zigzag encoding pattern:
	// Positive: encoded = 2 * value
	// Negative: encoded = 2 * |value| - 1

	testCases := []struct {
		originalValue int
		encodedValue  uint64 // The unsigned varint encoding
		expectedBytes []byte // Manually specified encoding for values that BytesToVarInt might mishandle
	}{
		{0, 0, []byte{0x00}},
		{1, 2, nil},
		{-1, 1, nil},
		{2, 4, nil},
		{-2, 3, nil},
		{3, 6, nil},
		{-3, 5, nil},
		{63, 126, nil},
		{-63, 125, nil},
		{64, 128, []byte{0x80, 0x01}}, // 128 = 0 | (1<<7), encoded as varint: 0x80 0x01
		{-64, 127, nil},
		{72, 144, []byte{0x90, 0x01}}, // 144 = 16 | (1<<7), encoded as varint: 0x90 0x01
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("value_%d", tc.originalValue), func(t *testing.T) {
			var encoded []byte

			// Use manually specified bytes if provided, otherwise generate
			if tc.expectedBytes != nil {
				encoded = tc.expectedBytes
			} else {
				encodedValue := uint64(tc.encodedValue)
				if encodedValue == 0 {
					encoded = []byte{0x00}
				} else if encodedValue < 128 {
					// Single byte
					encoded = []byte{byte(encodedValue)}
				} else {
					// Multi-byte: use BytesToVarInt
					encodedValueBytes := make([]byte, 4)
					binary.BigEndian.PutUint32(encodedValueBytes, uint32(encodedValue))
					encoded = BytesToVarInt(encodedValueBytes)
				}
			}

			// Decode using ReadVarIntSigned
			decoded, bytesRead := ReadVarIntSigned(0, encoded)

			if decoded != tc.originalValue {
				t.Errorf("expected %d, got %d (encoded as %d, bytes: %v)",
					tc.originalValue, decoded, tc.encodedValue, encoded)
			}

			if bytesRead != len(encoded) {
				t.Errorf("expected %d bytes read, got %d", len(encoded), bytesRead)
			}
		})
	}
}
