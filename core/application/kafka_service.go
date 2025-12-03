package application

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
)

// KafkaService implements the driving port (KafkaHandler interface).
// This is the application core that contains the business logic.
// Rule 2: The application implements the port defined by the core.
type KafkaService struct {
	parser parser.ProtocolParser
}

// NewKafkaService creates a new Kafka service that implements the driving port
func NewKafkaService(parser parser.ProtocolParser) driving.KafkaHandler {
	return &KafkaService{
		parser: parser,
	}
}

// HandleRequest processes a Kafka request and returns a response.
// This is where the core business logic lives.
func (s *KafkaService) HandleRequest(req domain.Request) (domain.Response, error) {
	// Parse the request using the protocol parser (infrastructure concern)
	parsedReq, err := s.parser.ParseRequest(req.Data)
	if err != nil {
		return domain.Response{}, err
	}

	errorCode := s.determineErrorCode(parsedReq.APIVersion)

	// Build response data structure
	responseData := &parser.ResponseData{
		CorrelationID:      parsedReq.CorrelationID,
		ErrorCode:          errorCode,
		ApiKeysArrayLength: []byte{0x02},
		ApiKey:             []byte{0x00, 0x12},
		MinVersion:         []byte{0x00, 0x00},
		MaxVersion:         []byte{0x00, 0x04},
		TagBuffer:          []byte{0x00},
	}

	// Encode the response using the protocol parser (infrastructure concern)
	encodedResponse, err := s.parser.EncodeResponse(responseData)
	if err != nil {
		return domain.Response{}, err
	}

	return domain.Response{
		Data: encodedResponse,
	}, nil
}

// determineErrorCode contains the business logic for error code determination.
// This is a domain rule, so it belongs in the core.
func (s *KafkaService) determineErrorCode(apiVersion int) []byte {
	errorCodeBuffer := make([]byte, 2)
	if apiVersion < 0 || apiVersion > 4 {
		// Business rule: Return error code 35 for unsupported API versions
		binary.BigEndian.PutUint16(errorCodeBuffer, uint16(35))
	}
	return errorCodeBuffer
}
