package fetch_service

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	cluster_metadata_repository "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/cluster_metadata"
	fetch_repository "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/fetch"
	port_repo "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/partition_file_repository"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/common"
)

type FetchService struct {
	parser                    parser.FetchParser
	fetch_repository          fetch_repository.FetchRepository
	metadata_repository       cluster_metadata_repository.ClusterMetadataRepository
	partition_file_repository port_repo.PartitionFileRepository
}

func NewFetchService(parser parser.FetchParser, repository fetch_repository.FetchRepository, metadata_repository cluster_metadata_repository.ClusterMetadataRepository, partition_file_repository port_repo.PartitionFileRepository) driving.KafkaHandler {
	return &FetchService{
		parser:                    parser,
		fetch_repository:          repository,
		metadata_repository:       metadata_repository,
		partition_file_repository: partition_file_repository,
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
		fmt.Println(">>>>>>>> there is no cluster metadata", err.Error())
	}
	_ = clusterMetaData

	messageFetchRequest := s.getMessageRequestForValidTopics(&topicFetchResponse, clusterMetaData)
	s.partition_file_repository.GetPartitionMessage(messageFetchRequest)

	// Encode the response using the protocol parser (infrastructure concern)
	encodedResponse, err := s.parser.EncodeResponse(&topicFetchResponse)
	if err != nil {
		return domain.Response{}, err
	}

	return domain.Response{
		Data: encodedResponse,
	}, nil
}

func (s *FetchService) getMessageRequestForValidTopics(topicFetchResponse *domain.ResponseDataFetch, clusterMetaData cluster_metadata_repository.ClusterMetadataRepositoryResponse) domain.MessageFetchRequest {
	retVal := domain.MessageFetchRequest{PartitionsToFetch: make([]domain.PartitionToFetch, 0)}

	for _, topic := range topicFetchResponse.Topics {
		partitionMetadataArray := clusterMetaData.TopicUUIDPartitionMetadataMap[topic.TopicName]
		topicMetadata := clusterMetaData.TopicUUIDTopicMetadataInfoMap[topic.TopicName]
		for partitionIndex, partition := range topic.Partitions {
			if partitionMetadataArray == nil || partitionIndex >= len(partitionMetadataArray) {
				partition.ErrorCode = 100
				continue
			}
			errorCode := int16(common.BytesToInt(partitionMetadataArray[partitionIndex].ErrorCode))
			partition.ErrorCode = errorCode
			if errorCode == 0 {
				retVal.PartitionsToFetch = append(retVal.PartitionsToFetch, domain.PartitionToFetch{
					TopicName:          topicMetadata.TopicNameInfo.TopicName,
					PartitionIndex:     partitionIndex,
					TopicFetchResponse: partition,
				})
			}
		}
	}
	return retVal
}
