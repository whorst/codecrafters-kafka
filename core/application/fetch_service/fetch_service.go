package fetch_service

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	cluster_metadata_repository "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/cluster_metadata"
	fetch_repository "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/fetch"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/common"
)

type FetchService struct {
	parser              parser.FetchParser
	fetch_repository    fetch_repository.FetchRepository
	metadata_repository cluster_metadata_repository.ClusterMetadataRepository
}

func NewFetchService(parser parser.FetchParser, repository fetch_repository.FetchRepository, metadata_repository cluster_metadata_repository.ClusterMetadataRepository) driving.KafkaHandler {
	return &FetchService{
		parser:              parser,
		fetch_repository:    repository,
		metadata_repository: metadata_repository,
	}
}

func (s *FetchService) HandleRequest(req domain.Request) (domain.Response, error) {
	parsedReq, err := s.parser.ParseRequest(req.Data)
	if err != nil {
		fmt.Println(">>>>>>>> ", err.Error())
		return domain.Response{}, err
	}

	// Build response data structure
	topicFetchResponse, err := s.fetch_repository.GetTopicFetch(*parsedReq)
	if err != nil {
		fmt.Println(">>>>>>>> ", err.Error())
		panic("panic")
	}

	clusterMetaData, err := s.metadata_repository.GetClusterMetadata()
	if err != nil {
		fmt.Println(">>>>>>>> ", err.Error())
		panic("panic")
	}
	_ = clusterMetaData

	for _, topic := range topicFetchResponse.Topics {
		partitionMetadataArray := clusterMetaData.TopicUUIDPartitionMetadataMap[topic.TopicName]
		for partitionIndex, partition := range topic.Partitions {
			if partitionMetadataArray == nil || partitionIndex >= len(partitionMetadataArray) {
				partition.ErrorCode = 100
				continue
			}
			partition.ErrorCode = int16(common.BytesToInt(partitionMetadataArray[partitionIndex].ErrorCode))
		}
	}

	// Encode the response using the protocol parser (infrastructure concern)
	encodedResponse, err := s.parser.EncodeResponse(&topicFetchResponse)
	if err != nil {
		return domain.Response{}, err
	}

	return domain.Response{
		Data: encodedResponse,
	}, nil
}
