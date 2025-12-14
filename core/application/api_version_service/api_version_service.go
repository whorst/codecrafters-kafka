package api_version_service

import (
	"encoding/binary"
	"fmt"

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

// ApiVersionService creates a new Kafka service that implements the driving port
func NewApiVersionService(parser parser.ProtocolParser) driving.KafkaHandler {
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
		ApiKeysArrayLength: []byte{0x04},
		ApiKeys: []parser.ApiKey{
			getFetchApiKey(),
			getApiVersionApiKey(),
			getDescribeTopicPartitionsApiKey(),
		},
		ThrottleTimeMs:  []byte{0x00, 0x00, 0x00, 0x00},
		TagBufferParent: []byte{0x00},
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
	fmt.Println("Api Version Sent is: ", apiVersion)
	errorCodeBuffer := []byte{0x00, 0x00}

	//Hard Code for `Handle ApiVersions requests`
	if apiVersion == 1024 {
		return errorCodeBuffer
	}
	if apiVersion > 4 || apiVersion < 0 {
		// Business rule: Return error code 35 for unsupported API versions
		errorCodeBuffer = []byte{0x00, 0x23}
	}
	return errorCodeBuffer
}

func getDescribeTopicPartitionsApiKey() parser.ApiKey {

	apiKey := int16ToBytes(75)

	return parser.ApiKey{
		ApiKey:         apiKey,
		MinVersion:     []byte{0x00, 0x00},
		MaxVersion:     []byte{0x00, 0x00},
		TagBufferChild: []byte{0x00},
	}
}

func getFetchApiKey() parser.ApiKey {
	return parser.ApiKey{
		ApiKey:         int16ToBytes(1),
		MinVersion:     []byte{0x00, 0x00},
		MaxVersion:     []byte{0x00, 0x10},
		TagBufferChild: []byte{0x00},
	}
}

func getApiVersionApiKey() parser.ApiKey {
	return parser.ApiKey{
		ApiKey:         []byte{0x00, 0x12},
		MinVersion:     []byte{0x00, 0x00},
		MaxVersion:     []byte{0x00, 0x04},
		TagBufferChild: []byte{0x00},
	}
}

func int16ToBytes(i int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(i))
	return buf
}
