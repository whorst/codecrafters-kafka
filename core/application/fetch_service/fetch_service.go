package fetch_service

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	fetch_repository "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/fetch"
)

type FetchService struct {
	parser           parser.FetchParser
	fetch_repository fetch_repository.FetchRepository
}

func NewFetchService(parser parser.FetchParser, repository fetch_repository.FetchRepository) driving.KafkaHandler {
	return &FetchService{
		parser:           parser,
		fetch_repository: repository,
	}
}

func (s *FetchService) HandleRequest(req domain.Request) (domain.Response, error) {

	parsedReq, err := s.parser.ParseRequest(req.Data)
	if err != nil {
		fmt.Println(">>>>>>>> ", err.Error())
		return domain.Response{}, err
	}

	// Build response data structure
	responseData, err := s.fetch_repository.GetTopicFetch(*parsedReq)
	if err != nil {
		fmt.Println(">>>>>>>> ", err.Error())
	}
	fmt.Println(">>>>>>>>>>>> here")
	// Encode the response using the protocol parser (infrastructure concern)
	encodedResponse, err := s.parser.EncodeResponse(&responseData)
	if err != nil {
		return domain.Response{}, err
	}

	return domain.Response{
		Data: encodedResponse,
	}, nil
}
