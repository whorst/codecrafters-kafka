package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
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

	clientIdLength := p.parseBytesToInt(data[12:14])
	clientId := data[14 : 14+clientIdLength]
	_ = clientId

	arrayLengthOffset := 14 + clientIdLength + 1 //We add + 1 here to skip the tags bugger that comes after the clientId
	topicArrayLength, totalBytesRead := p.readVarInt(arrayLengthOffset, data)
	topicArrayLength -= 1 // This always will always arrive with 1 added to it for some reason

	topicArrayOffset := arrayLengthOffset + totalBytesRead
	parsedTopics := []parser.ParsedTopic{}

	for range topicArrayLength {
		topicNameLength, newTotalBytesRead := p.readVarInt(topicArrayOffset, data)
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

	messageSizeBuffer := make([]byte, 4)
	//
	binary.BigEndian.PutUint32(messageSizeBuffer, uint32(response.GetMessageSize()))
	//
	responseData = append(responseData, messageSizeBuffer...)

	responseData = append(responseData, response.CorrelationID...)
	responseData = append(responseData, response.ResponseDataDescribeTopicHeader.TagBufferHeader...)

	responseData = append(responseData, response.ResponseDataDescribeTopicBody.ThrottleTimeMs...)

	// Convert this to a varint
	numberOfTopics := byte(uint8(len(response.ResponseDataDescribeTopicBody.Topics) + 1)) // The responseData number of topics should be able to be represented by one byte
	responseData = append(responseData, numberOfTopics)

	for _, topic := range response.Topics {
		responseData = append(responseData, topic.ErrorCode...)

		// Convert this to a varint
		responseData = append(responseData, byte(uint8(len(topic.TopicNameInfo.TopicNameBytes)+1)))
		responseData = append(responseData, topic.TopicNameInfo.TopicNameBytes...)
		responseData = append(responseData, topic.TopicId...)
		responseData = append(responseData, topic.IsInternal...)
		responseData = append(responseData, topic.Partitions...)
		responseData = append(responseData, topic.TopicAuthorizedOperations...)
		responseData = append(responseData, topic.TagBuffer...)
	}
	responseData = append(responseData, response.NextCursor...)
	responseData = append(responseData, response.TagBufferBody...)

	return responseData, nil
}

func (p *KafkaProtocolParserDescribeTopic) parseBytesToInt(dataBytes []byte) int {
	var retVal int
	if len(dataBytes) > 5 {
		panic("Input too Large")
	}
	if len(dataBytes) == 1 {
		retVal = int(dataBytes[0])
	}

	if len(dataBytes) > 1 && len(dataBytes) < 5 {
		for idx := range dataBytes {
			retVal = retVal << 8
			retVal |= int(dataBytes[idx])
		}
	}
	return retVal
}

func (p *KafkaProtocolParserDescribeTopic) readVarInt(offset int, header []byte) (int, int) {
	var headerSize byte
	mostSignificantBit := 1
	var total uint64 = 0
	var numberOfBytesRead int = 0

	for mostSignificantBit >= 1 {
		if err := binary.Read(bytes.NewReader(header[offset:]), binary.BigEndian, &headerSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return 0, 0
		}
		// Most significant bit is a flag that checks if there's another byte to consume after this, 1 means yes and 0 means no
		mostSignificantBit = int(headerSize & byte(0x80))
		// These are the value bits that we add to total. We accomplish this by shifting left total and doing an or operations
		// on the value bits
		valueBits := uint64(headerSize & byte(0x7f))
		total = total<<7 | valueBits
		offset += 1
		numberOfBytesRead += 1
	}
	return int(total), numberOfBytesRead
}
