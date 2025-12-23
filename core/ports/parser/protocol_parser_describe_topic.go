package parser

import (
	"github.com/codecrafters-io/kafka-starter-go/core/domain"
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
	TopicsUnknown  []ResponseDataDescribeTopicInfo
	Topics         []ResponseDataDescribeTopicInfo
	NextCursor     []byte // Nullable 1 byte, use -1 for null
	TagBufferBody  []byte // Hard Coded 1 Byte
}

type ResponseDataDescribeTopicInfo struct {
	ErrorCode                 []byte        //2 bytes
	TopicNameInfo             TopicNameInfo // From the request?
	TopicId                   []byte        // UUID
	IsInternal                []byte        // 1 byte, hard coded to 00
	Partitions                []*domain.PartitionMetadata
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
