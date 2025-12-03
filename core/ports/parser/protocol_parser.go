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
	CorrelationID      []byte // Correlation ID (Unknown)
	ErrorCode          []byte // Error code (2 bytes)
	ApiKeysArrayLength []byte // Hard Coded to 1 byte
	// ================ Everything below will eventually be a part of an array ================
	ApiKey         []byte // Hard Coded to 2 byte
	MinVersion     []byte // Hard coded to 2 bytes
	MaxVersion     []byte // Hard Coded to 2 bytes
	TagBufferChild []byte // Hard Coded to 1 byte
	// ================ End Array ==========================
	ThrottleTimeMs  []byte // Hard Coded to 4 bytes
	TagBufferParent []byte // Hard Coded to 1 byte
}

func (ResponseData ResponseData) GetMessageSize() int {
	totalLength := len(ResponseData.CorrelationID)
	totalLength += len(ResponseData.ErrorCode)
	totalLength += len(ResponseData.ApiKeysArrayLength)
	totalLength += len(ResponseData.ApiKey)
	totalLength += len(ResponseData.MinVersion)
	totalLength += len(ResponseData.MaxVersion)
	totalLength += len(ResponseData.TagBufferChild)
	totalLength += len(ResponseData.ThrottleTimeMs)
	totalLength += len(ResponseData.TagBufferParent)
	return totalLength
}
