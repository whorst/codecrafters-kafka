package fetch_service

import (
	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/fetch"
)

type FetchService struct {
	parser fetch.FetchParser
}

func NewFetchService(parser fetch.FetchParser) driving.KafkaHandler {
	return &FetchService{
		parser: parser,
	}
}

func (s *FetchService) HandleRequest(req domain.Request) (domain.Response, error) {

	parsedReq, err := s.parser.ParseRequest(req.Data)
	if err != nil {
		return domain.Response{}, err
	}

	// Build response data structure
	responseData := &fetch.ResponseDataFetch{
		CorrelationID:  parsedReq.CorrelationID,
		ThrottleTimeMs: 0,  // Throttle time in milliseconds
		ErrorCode:      0,  // Error code (0 = no error)
		SessionID:      0,  // Session ID
		Topics:         []fetch.FetchResponseTopic{}, // Empty topics array for now
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
