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
	CorrelationID []byte // Correlation ID (Unknown)
	ErrorCode     []byte // Error code (2 bytes)
	ApiKeys       []byte // Hard Coded to 1 byte
	ApiKey        []byte // Hard Coded to 2 byte
	MinVersion    []byte // Hard coded to 2 bytes
	MaxVersion    []byte // HArd Coded to 2 bytes
}
