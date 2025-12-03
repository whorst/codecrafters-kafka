package parser

type ProtocolParser interface {
	// ParseRequest extracts structured data from raw binary request data
	ParseRequest(data []byte) (*ParsedRequest, error)

	// EncodeResponse converts a response into binary format
	EncodeResponse(response *ResponseData) ([]byte, error)
}

// ParsedRequest represents a parsed Kafka request
type ParsedRequest struct {
	CorrelationID []byte
	APIVersion    int
}

// ResponseData represents the data needed to build a response
type ResponseData struct {
	Size          []byte // Response size (4 bytes)
	CorrelationID []byte // Correlation ID (4 bytes)
	ErrorCode     []byte // Error code (2 bytes)
}
