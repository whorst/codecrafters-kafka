package parser

import (
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/partition_metadata"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/common"
)

// KafkaProtocolParser is a parser adapter that implements the ProtocolParser port.
// This handles the technical concern of binary parsing/encoding for the Kafka protocol.
// Rule 2: Adapters implement the ports defined by the core.
// Rule 3: Dependencies point inward - this adapter depends on the core port.
type KafkaProtocolParserDescribeTopic struct{}

func NewKafkaProtocolParserDescribeTopic() parser.ProtocolParserDescribeTopic {
	return &KafkaProtocolParserDescribeTopic{}
}

func (p *KafkaProtocolParserDescribeTopic) ParseRequest(data []byte) (*parser.ParsedRequestDescribeTopic, error) {
	if len(data) < 12 {
		return nil, ErrInvalidRequest
	}

	correlationID := []byte{data[8], data[9], data[10], data[11]}

	clientIdLength := common.BytesToInt(data[12:14])
	clientId := data[14 : 14+clientIdLength]
	_ = clientId

	arrayLengthOffset := 14 + clientIdLength + 1 //We add + 1 here to skip the tags bugger that comes after the clientId
	topicArrayLength, totalBytesRead := common.ReadVarIntUnsigned(arrayLengthOffset, data)
	topicArrayLength -= 1 // This always will always arrive with 1 added to it for some reason

	topicArrayOffset := arrayLengthOffset + totalBytesRead
	parsedTopics := []parser.ParsedTopic{}

	for range topicArrayLength {
		topicNameLength, newTotalBytesRead := common.ReadVarIntUnsigned(topicArrayOffset, data)
		topicNameLength -= 1 // This always will always arrive with 1 added to it for some reaso
		topicArrayOffset += newTotalBytesRead

		topicNameOffsetEnd := topicArrayOffset + topicNameLength
		topicNameBytes := data[topicArrayOffset:topicNameOffsetEnd]
		topicNameHumanReadable := string(topicNameBytes)
		topicArrayOffset += topicNameLength

		topicArrayOffset += 1 // Skip the topic tag buffer

		parsedTopicData := parser.ParsedTopic{
			TopicNameBytes: topicNameBytes,
			TopicName:      topicNameHumanReadable,
			TagBuffer:      []byte{0x00},
		}
		parsedTopics = append(parsedTopics, parsedTopicData)
	}

	return &parser.ParsedRequestDescribeTopic{CorrelationIdBytes: correlationID, Topics: parsedTopics, TagBuffer: []byte{0x00}}, nil
}

func (p *KafkaProtocolParserDescribeTopic) EncodeResponse(response *parser.ResponseDataDescribeTopic) ([]byte, error) {
	responseData := []byte{}

	responseData = append(responseData, response.CorrelationID...)
	responseData = append(responseData, response.ResponseDataDescribeTopicHeader.TagBufferHeader...)

	responseData = append(responseData, response.ResponseDataDescribeTopicBody.ThrottleTimeMs...)

	// Convert this to a varint
	numberOfTopics := byte(uint8(len(response.ResponseDataDescribeTopicBody.Topics) + 1)) // The responseData number of topics should be able to be represented by one byte
	numberOfTopicsVarInt := common.BytesToVarInt([]byte{numberOfTopics})
	responseData = append(responseData, numberOfTopicsVarInt...)

	for _, topic := range response.Topics {
		responseData = append(responseData, topic.ErrorCode...)

		// Convert this to a varint
		topicNameBytes := []byte{byte(uint8(len(topic.TopicNameInfo.TopicNameBytes) + 1))}
		topicNameLengthVarInt := common.BytesToVarInt(topicNameBytes)

		responseData = append(responseData, topicNameLengthVarInt...)
		responseData = append(responseData, topic.TopicNameInfo.TopicNameBytes...)
		responseData = append(responseData, topic.TopicId...)
		responseData = append(responseData, topic.IsInternal...)
		responseData = append(responseData, p.encodeAllPartitions(topic.Partitions)...)
		responseData = append(responseData, topic.TopicAuthorizedOperations...)
		responseData = append(responseData, topic.TagBuffer...)
	}
	responseData = append(responseData, response.NextCursor...)
	responseData = append(responseData, response.TagBufferBody...)

	return responseData, nil
}

func (p *KafkaProtocolParserDescribeTopic) encodeAllPartitions(allPartitionMetadata []*partition_metadata.PartitionMetadata) []byte {
	retVal := []byte{}
	if len(allPartitionMetadata) == 0 {
		return []byte{0x01}
	}

	retVal = append(retVal, common.IntToVarInt(len(allPartitionMetadata)+1)...)

	for _, pmd := range allPartitionMetadata {
		retVal = append(retVal, p.encodePartition(pmd)...)
	}

	return retVal
}

func (p *KafkaProtocolParserDescribeTopic) encodePartition(pm *partition_metadata.PartitionMetadata) []byte {
	result := []byte{}

	// Error Code (2 bytes)
	result = append(result, pm.ErrorCode...)

	// Partition Index (4 bytes - INT32)
	result = append(result, pm.PartitionIndex...)

	// Leader ID (4 bytes - INT32)
	result = append(result, pm.LeaderId...)

	// Leader Epoch (4 bytes - INT32)
	result = append(result, pm.LeaderEpoch...)

	// Replica Nodes
	// Array Length (varint) - stored as bytes, use directly
	result = append(result, pm.ReplicaNodes.ArrayLength...)
	// Replica Node array (each node is 4 bytes)
	result = append(result, pm.ReplicaNodes.ReplicaNodesArray...)

	// ISR Nodes
	// Array Length (varint) - stored as bytes, use directly
	result = append(result, pm.IsrNodes.ArrayLength...)
	// ISR Node array (each node is 4 bytes)
	result = append(result, pm.IsrNodes.IsrNodeArray...)

	// Eligible Leader Replicas
	// Array Length (varint) - stored as bytes, use directly
	result = append(result, pm.EligibleLeaderReplicasArrayLength...)

	// Last Known ELR
	// Array Length (varint) - stored as bytes, use directly
	result = append(result, pm.LastKnownElrArrayLength...)

	// Offline Replicas
	// Array Length (varint) - stored as bytes, use directly
	result = append(result, pm.OfflineReplicasArrayLength...)

	// Tag Buffer (1 byte)
	result = append(result, pm.TagBuffer...)

	return result
}
