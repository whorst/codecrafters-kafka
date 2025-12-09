package kafka_describe_topic_service

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
type KafkaDescribeService struct {
	parser parser.ProtocolParserDescribeTopic
}

// NewKafkaService creates a new Kafka service that implements the driving port
func NewKafkaDescribeTopicService(parser parser.ProtocolParserDescribeTopic) driving.KafkaHandler {
	return &KafkaDescribeService{
		parser: parser,
	}
}

var HardCodedTopicId = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

// HandleRequest processes a Kafka request and returns a response.
// This is where the core business logic lives.
func (s *KafkaDescribeService) HandleRequest(req domain.Request) (domain.Response, error) {
	fmt.Printf("This is the request %+v \n", req.Data)

	// Parse the request using the protocol parser (infrastructure concern)
	parsedReq, err := s.parser.ParseRequest(req.Data)
	if err != nil {
		return domain.Response{}, err
	}

	responseInfo := []parser.ResponseDataDescribeTopicInfo{}

	for _, topicData := range parsedReq.Topics {
		newInfo := parser.ResponseDataDescribeTopicInfo{
			ErrorCode:                 []byte{0x00, 0x03},                                                                             // []byte //2 bytes
			TopicNameInfo:             parser.TopicNameInfo{TopicName: topicData.TopicName, TopicNameBytes: topicData.TopicNameBytes}, // string // From the request?
			TopicId:                   HardCodedTopicId,                                                                               // string // UUID
			IsInternal:                []byte{0x00},                                                                                   // []byte // 1 byte, hard coded to 00
			Partitions:                []byte{0x01},                                                                                   // []byte // 1 byte, hard coded to 01
			TopicAuthorizedOperations: []byte{0x00, 0x00, 0x00, 0x00},                                                                 // []byte // 4 bytes, hard coded to 00
			TagBuffer:                 []byte{0x00},                                                                                   // []byte // Hard Coded to 1 byte, 00
		}
		responseInfo = append(responseInfo, newInfo)
	}

	responseData := &parser.ResponseDataDescribeTopic{
		ResponseDataDescribeTopicHeader: parser.ResponseDataDescribeTopicHeader{
			CorrelationID:   parsedReq.CorrelationIdBytes,
			TagBufferHeader: []byte{0x00},
		},
		ResponseDataDescribeTopicBody: parser.ResponseDataDescribeTopicBody{
			ThrottleTimeMs: []byte{0x00, 0x00, 0x00, 0x00},
			Topics:         responseInfo,
			NextCursor:     []byte{0xff},
			TagBufferBody:  []byte{0x00},
		},
	}

	// Encode the response using the protocol parser (infrastructure concern)
	encodedResponse, err := s.parser.EncodeResponse(responseData)
	if err != nil {
		return domain.Response{}, err
	}
	fmt.Printf("This is the response %+v \n", encodedResponse)

	return domain.Response{
		Data: encodedResponse,
	}, nil
}

func getDescribeTopicPartitionsApiKey() parser.ApiKey {
	return parser.ApiKey{
		ApiKey:         int16ToBytes(75),
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
