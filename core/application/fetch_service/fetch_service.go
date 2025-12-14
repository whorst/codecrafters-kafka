package fetch_service

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
)

type FetchService struct {
	parser parser.ProtocolParser
}

func NewFetchService(parser parser.ProtocolParser) driving.KafkaHandler {
	return &FetchService{
		parser: parser,
	}
}

func (s *FetchService) HandleRequest(req domain.Request) (domain.Response, error) {

	parsedReq, err := s.parser.ParseRequest(req.Data)
	if err != nil {
		return domain.Response{}, err
	}

	errorCode := s.determineErrorCode(parsedReq.APIVersion)

	// Build response data structure
	responseData := &parser.ResponseData{
		CorrelationID:      parsedReq.CorrelationID,
		ErrorCode:          errorCode,
		ApiKeysArrayLength: []byte{0x03},
		ApiKeys: []parser.ApiKey{
			getFetchApiKey(),
			{
				ApiKey:         []byte{0x00, 0x12},
				MinVersion:     []byte{0x00, 0x00},
				MaxVersion:     []byte{0x00, 0x04},
				TagBufferChild: []byte{0x00},
			},
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

	return domain.Response{Data: make([]byte, 0)}, nil
}

// determineErrorCode contains the business logic for error code determination.
// This is a domain rule, so it belongs in the core.
func (s *FetchService) determineErrorCode(apiVersion int) []byte {
	fmt.Println("Api Version Sent is: ", apiVersion)
	errorCodeBuffer := []byte{0x00, 0x00}

	//Hard Code for `Handle ApiVersions requests`
	if apiVersion == 1024 {
		return errorCodeBuffer
	}
	if apiVersion > 16 || apiVersion < 0 {
		// Business rule: Return error code 35 for unsupported API versions
		errorCodeBuffer = []byte{0x00, 0x23}
	}
	return errorCodeBuffer
}

func getFetchApiKey() parser.ApiKey {
	return parser.ApiKey{
		ApiKey:         int16ToBytes(1),
		MinVersion:     []byte{0x00, 0x00},
		MaxVersion:     []byte{0x00, 0x00},
		TagBufferChild: []byte{0x00},
	}
}

func int16ToBytes(i int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(i))
	return buf
}
