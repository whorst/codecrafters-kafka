package parser

import (
	"github.com/codecrafters-io/kafka-starter-go/core/ports/partition_metadata"
)

type ProtocolParserDescribeTopic interface {
	// ParseRequest extracts structured data from raw binary request data
	ParseRequest(data []byte) (*ParsedRequestDescribeTopic, error)

	// EncodeResponse converts a response into binary format
	EncodeResponse(response *ResponseDataDescribeTopic) ([]byte, error)
}

// ParsedRequest represents a parsed Kafka request
type ParsedRequestDescribeTopic struct {
	CorrelationIdBytes []byte
	Topics             []ParsedTopic
	TagBuffer          []byte
}

type ParsedTopic struct {
	TopicName      string
	TagBuffer      []byte
	TopicNameBytes []byte
}

type ResponseDataDescribeTopicHeader struct {
	CorrelationID   []byte // Correlation ID 4 Bytes, Matches the Request
	TagBufferHeader []byte // Hard Coded to 1 byte
}

type ResponseDataDescribeTopicBody struct {
	ThrottleTimeMs []byte // Hard Coded 4 Bytes, use 0
	Topics         []ResponseDataDescribeTopicInfo
	NextCursor     []byte // Nullable 1 byte, use -1 for null
	TagBufferBody  []byte // Hard Coded 1 Byte
}

type ResponseDataDescribeTopicInfo struct {
	ErrorCode                 []byte        //2 bytes
	TopicNameInfo             TopicNameInfo // From the request?
	TopicId                   []byte        // UUID
	IsInternal                []byte        // 1 byte, hard coded to 00
	Partitions                []*partition_metadata.PartitionMetadata
	TopicAuthorizedOperations []byte // 4 bytes, hard coded to 00
	NextCursor                []byte // 1 byte, hard coded to 0xff
	TagBuffer                 []byte // Hard Coded to 1 byte, 00
}

type TopicNameInfo struct {
	TopicName      string
	TopicNameBytes []byte
}

// ResponseData represents the data needed to build a response
type ResponseDataDescribeTopic struct {
	ResponseDataDescribeTopicHeader
	ResponseDataDescribeTopicBody
}

func (r ResponseDataDescribeTopic) GetMessageSize() int {
	total := 0
	total += len(r.CorrelationID)
	total += len(r.ResponseDataDescribeTopicHeader.TagBufferHeader)

	total += len(r.ResponseDataDescribeTopicBody.ThrottleTimeMs)
	numberOfTopics := uint8(len(r.ResponseDataDescribeTopicBody.Topics)) // The total number of topics should be able to be represented by one byte
	_ = numberOfTopics
	total += 1

	for _, topic := range r.Topics {
		total += len(topic.ErrorCode)
		total += 1 // How many bytes is the int length of the topic name?
		total += len(topic.TopicNameInfo.TopicNameBytes)
		total += len(topic.TopicId)
		total += len(topic.IsInternal) //represent topic.isInternal
		total += len(topic.Partitions)
		total += len(topic.TopicAuthorizedOperations)
		total += len(topic.TagBuffer)
	}
	total += len(r.NextCursor)
	total += len(r.TagBufferBody)

	return total
}
