package parser

import (
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
)

// KafkaProtocolParser is a parser adapter that implements the ProtocolParser port.
// This handles the technical concern of binary parsing/encoding for the Kafka protocol.
// Rule 2: Adapters implement the ports defined by the core.
// Rule 3: Dependencies point inward - this adapter depends on the core port.
type KafkaProtocolParser struct{}

// NewKafkaProtocolParser creates a new Kafka protocol parser
func NewKafkaProtocolParser() parser.ProtocolParser {
	return &KafkaProtocolParser{}
}

func (p *KafkaProtocolParser) ParseRequest(data []byte) (*parser.ParsedRequest, error) {
	if len(data) < 12 {
		return nil, ErrInvalidRequest
	}

	correlationID := []byte{data[8], data[9], data[10], data[11]}
	apiVersionBytes := []byte{data[6], data[7]}
	apiVersion := p.parseBytesToInt(apiVersionBytes)

	return &parser.ParsedRequest{
		CorrelationID: correlationID,
		APIVersion:    apiVersion,
	}, nil
}

func (p *KafkaProtocolParser) EncodeResponse(response *parser.ResponseData) ([]byte, error) {
	responseData := make([]byte, 0, 10)
	responseData = append(responseData, response.Size...)
	responseData = append(responseData, response.CorrelationID...)
	responseData = append(responseData, response.ErrorCode...)
	responseData = append(responseData, response.ApiKeys...)
	responseData = append(responseData, response.ApiKeys...)
	responseData = append(responseData, response.MinVersion...)
	responseData = append(responseData, response.MaxVersion...)
	return responseData, nil
}

func (p *KafkaProtocolParser) parseBytesToInt(dataBytes []byte) int {
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

// ErrInvalidRequest is returned when the request data is invalid
var ErrInvalidRequest = &ParseError{Message: "invalid request: insufficient data"}

// ParseError represents a parsing error
type ParseError struct {
	Message string
}

func (e *ParseError) Error() string {
	return e.Message
}
