package application

import "encoding/binary"

func getCorrelationIdFromMessageHeader(dataBytes []byte) []byte {
	return []byte{dataBytes[8], dataBytes[9], dataBytes[10], dataBytes[11]}
}

func getRequestApiVersionFromMessageHeader(dataBytes []byte) []byte {
	return []byte{dataBytes[6], dataBytes[7]}
}

func parseBytesToInt(dataBytes []byte) int {
	var retVal int
	if len(dataBytes) > 5 {
		panic("Input too Large")
	}
	if len(dataBytes) == 1 {
		retVal = int(dataBytes[0])
	}
	if len(dataBytes) > 1 && len(dataBytes) < 5 {
		for idx := range dataBytes {
			retVal |= int(dataBytes[idx]) << 8
		}
	}
	return retVal
}

func getErrorCode(apiVersion int) []byte {
	errorCodeBuffer := make([]byte, 2)
	if apiVersion < 0 || apiVersion > 4 {
		binary.BigEndian.PutUint16(errorCodeBuffer, uint16(35))
	}

	return errorCodeBuffer
}
