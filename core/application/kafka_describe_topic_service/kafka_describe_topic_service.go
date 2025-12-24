package kafka_describe_topic_service

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	cluster_metadata_port "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/cluster_metadata"
)

// KafkaService implements the driving port (KafkaHandler interface).
// This is the application core that contains the business logic.
// Rule 2: The application implements the port defined by the core.
type KafkaDescribeService struct {
	parser                  parser.ProtocolParserDescribeTopic
	cluster_metadata_parser cluster_metadata_port.ClusterMetadataRepository
}

// NewKafkaService creates a new Kafka service that implements the driving port
func NewKafkaDescribeTopicService(parser parser.ProtocolParserDescribeTopic, metadata_parser cluster_metadata_port.ClusterMetadataRepository) driving.KafkaHandler {
	return &KafkaDescribeService{
		parser:                  parser,
		cluster_metadata_parser: metadata_parser,
	}
}

var HardCodedTopicId = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

// HandleRequest processes a Kafka request and returns a response.
// This is where the core business logic lives.
func (s *KafkaDescribeService) HandleRequest(req domain.Request) (domain.Response, error) {
	fmt.Printf("This is the request %+v \n", req.Data)

	// Parse the request using the protocol parser (infrastructure concern)
	parsedReqs, err := s.parser.ParseRequest(req.Data)
	if err != nil {
		return domain.Response{}, err
	}
	topicsToFind := map[string]parser.TopicNameInfo{}
	for _, parsedRequest := range parsedReqs.Topics {
		topicsToFind[parsedRequest.TopicName] = parser.TopicNameInfo{TopicNameBytes: parsedRequest.TopicNameBytes, TopicName: parsedRequest.TopicName}
		fmt.Printf("These are the topics to find: %+v\n", parsedRequest.TopicName)
	}

	topicResponseInfo := []parser.ResponseDataDescribeTopicInfo{}
	topicsUnknown := []parser.ResponseDataDescribeTopicInfo{}

	clusterMetadata, _ := s.cluster_metadata_parser.GetClusterMetadata()
	fmt.Printf("TopicUUIDTopicMetadataInfoMap: %+v\n", clusterMetadata.TopicUUIDTopicMetadataInfoMap)
	fmt.Printf("TopicUUIDPartitionMetadataMap: %+v\n", clusterMetadata.TopicUUIDPartitionMetadataMap)
	fmt.Printf("TopicNameTopicUuidMap: %+v\n", clusterMetadata.TopicNameTopicUuidMap)

	s.GetPartitionMetadataForTopic(clusterMetadata)

	//responseData := getOriginalResponse(parsedReq, topicResponseInfo)

	fmt.Printf("Topics to find: %+v \n", topicsToFind)

	topicsUnknown = s.GetTopicsNotFoundFromRequestData(topicsToFind, clusterMetadata, topicsUnknown)
	topicResponseInfo = s.GetTopicsFromRequestData(clusterMetadata, topicsToFind, topicResponseInfo)
	responseData := s.GetResponseDataDescribeTopic(parsedReqs, topicsUnknown, topicResponseInfo)

	// Encode the response using the protocol parser (infrastructure concern)
	encodedResponse, err := s.parser.EncodeResponse(responseData)
	messageSizeBuffer := make([]byte, 4)
	//
	binary.BigEndian.PutUint32(messageSizeBuffer, uint32(len(encodedResponse)))
	//
	encodedResponse = append(messageSizeBuffer, encodedResponse...)

	if err != nil {
		return domain.Response{}, err
	}
	fmt.Printf("This is the response %+v \n", encodedResponse)

	return domain.Response{
		Data: encodedResponse,
	}, nil
}

func (s *KafkaDescribeService) GetResponseDataDescribeTopic(parsedReqs *parser.ParsedRequestDescribeTopic, topicsUnknown []parser.ResponseDataDescribeTopicInfo, topicResponseInfo []parser.ResponseDataDescribeTopicInfo) *parser.ResponseDataDescribeTopic {
	responseData := &parser.ResponseDataDescribeTopic{
		ResponseDataDescribeTopicHeader: parser.ResponseDataDescribeTopicHeader{
			CorrelationID:   parsedReqs.CorrelationIdBytes,
			TagBufferHeader: []byte{0x00},
		},
		ResponseDataDescribeTopicBody: parser.ResponseDataDescribeTopicBody{
			ThrottleTimeMs: []byte{0x00, 0x00, 0x00, 0x00},
			TopicsUnknown:  topicsUnknown,
			Topics:         topicResponseInfo,
			NextCursor:     []byte{0xff},
			TagBufferBody:  []byte{0x00},
		},
	}
	return responseData
}

func (s *KafkaDescribeService) GetTopicsFromRequestData(clusterMetadata cluster_metadata_port.ClusterMetadataRepositoryResponse, topicsToFind map[string]parser.TopicNameInfo, topicResponseInfo []parser.ResponseDataDescribeTopicInfo) []parser.ResponseDataDescribeTopicInfo {
	for _, topicData := range clusterMetadata.TopicUUIDTopicMetadataInfoMap {
		fmt.Printf("Adding topic data to response for topic name: %+v\n", topicData.TopicNameInfo.TopicName)

		if _, exists := topicsToFind[topicData.TopicNameInfo.TopicName]; !exists {
			continue
		}

		topicInfo := parser.ResponseDataDescribeTopicInfo{
			ErrorCode:                 []byte{0x00, 0x00},             // []byte //2 bytes
			TopicNameInfo:             topicData.TopicNameInfo,        // string // From the request?
			TopicId:                   topicData.TopicId,              // string // UUID
			IsInternal:                []byte{0x00},                   // []byte // 1 byte, hard coded to 00
			Partitions:                topicData.PartitionsArray,      // []byte // 1 byte, hard coded to 01
			TopicAuthorizedOperations: []byte{0x00, 0x00, 0x00, 0x00}, // []byte // 4 bytes, hard coded to 00
			TagBuffer:                 []byte{0x00},                   // []byte // Hard Coded to 1 byte, 00
		}
		topicResponseInfo = append(topicResponseInfo, topicInfo)
	}

	sort.Slice(topicResponseInfo, func(i, j int) bool {
		return topicResponseInfo[i].TopicNameInfo.TopicName < topicResponseInfo[j].TopicNameInfo.TopicName
	})

	return topicResponseInfo
}

func (s *KafkaDescribeService) GetTopicsNotFoundFromRequestData(topicsToFind map[string]parser.TopicNameInfo, clusterMetadata cluster_metadata_port.ClusterMetadataRepositoryResponse, topicsUnknown []parser.ResponseDataDescribeTopicInfo) []parser.ResponseDataDescribeTopicInfo {
	for topicName, topicToFind := range topicsToFind {
		if _, exists := clusterMetadata.TopicNameTopicUuidMap[topicName]; !exists {

			fmt.Printf("Could Not find Topic: %+v \n", topicName)
			nonExistingInfo := parser.ResponseDataDescribeTopicInfo{
				ErrorCode:                 []byte{0x00, 0x03},             // []byte //2 bytes
				TopicNameInfo:             topicToFind,                    // string // From the request?
				TopicId:                   HardCodedTopicId,               // string // UUID
				IsInternal:                []byte{0x00},                   // []byte // 1 byte, hard coded to 00
				Partitions:                []*domain.PartitionMetadata{},  // []byte // 1 byte, hard coded to 01
				TopicAuthorizedOperations: []byte{0x00, 0x00, 0x00, 0x00}, // []byte // 4 bytes, hard coded to 00
				TagBuffer:                 []byte{0x00},                   // []byte // Hard Coded to 1 byte, 00
			}
			topicsUnknown = append(topicsUnknown, nonExistingInfo)
		} else {
			continue
		}
	}
	return topicsUnknown
}

func (s *KafkaDescribeService) GetPartitionMetadataForTopic(clusterMetadata cluster_metadata_port.ClusterMetadataRepositoryResponse) {
	for topicUuid, partitionMetadata := range clusterMetadata.TopicUUIDPartitionMetadataMap {
		if topicMetadata, exists := clusterMetadata.TopicUUIDTopicMetadataInfoMap[topicUuid]; !exists {
			panic("Partition Metadata has invalid topic")
		} else {
			topicMetadata.PartitionsArray = partitionMetadata
		}
	}
}
