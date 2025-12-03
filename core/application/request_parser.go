package application

func getCorrelationIdFromMessageHeader(dataBytes []byte) []byte {
	return []byte{dataBytes[8], dataBytes[9], dataBytes[10], dataBytes[11]}
}
