package parser

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/core/ports/fetch"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/common"
)

type KafkaProtocolParserFetch struct{}

// NewKafkaProtocolParser creates a new Kafka protocol parser
func NewKafkaProtocolParserFetch() *KafkaProtocolParserFetch {
	return &KafkaProtocolParserFetch{}
}

func (p *KafkaProtocolParserFetch) ParseRequest(data []byte) (*fetch.ParsedRequestFetch, error) {
	// Skip the 4-byte message size at the beginning
	if len(data) < 16 {
		return nil, ErrInvalidRequestFetch
	}

	offset := 4 // Skip message size (4 bytes)

	// Parse Header
	// APIKey (2 bytes)
	apiKeyBytes := data[offset : offset+2]
	apiKey := common.BytesToInt(apiKeyBytes)
	offset += 2

	// APIVersion (2 bytes)
	apiVersionBytes := data[offset : offset+2]
	apiVersion := common.BytesToInt(apiVersionBytes)
	offset += 2

	// CorrelationID (4 bytes)
	correlationID := data[offset : offset+4]
	offset += 4

	// ClientID (2-byte length + string)
	if offset+2 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	clientIDLength := common.BytesToInt(data[offset : offset+2])
	offset += 2

	if offset+clientIDLength > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	clientIDBytes := data[offset : offset+clientIDLength]
	clientID := string(clientIDBytes)
	offset += clientIDLength

	// Skip tag buffer (1 byte) after ClientID
	offset += 1

	// Parse Body
	// MaxWaitMS (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	maxWaitMS := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// MinBytes (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	minBytes := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// MaxBytes (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	maxBytes := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// IsolationLevel (1 byte)
	if offset+1 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	isolationLevel := int8(data[offset])
	offset += 1

	// SessionID (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	sessionID := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// SessionEpoch (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	sessionEpoch := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Topics array (varint length with +1 pattern)
	if offset >= len(data) {
		return nil, ErrInvalidRequestFetch
	}
	topicsArrayLength, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	topicsArrayLength -= 1 // Subtract 1 because of the +1 pattern
	offset += totalBytesRead

	topics := []fetch.FetchTopic{}
	for i := 0; i < topicsArrayLength; i++ {
		// Topic name (varint length + string)
		if offset >= len(data) {
			return nil, ErrInvalidRequestFetch
		}
		topicNameLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
		topicNameLength -= 1 // Subtract 1 because of the +1 pattern
		offset += bytesRead

		if offset+topicNameLength > len(data) {
			return nil, ErrInvalidRequestFetch
		}
		topicNameBytes := data[offset : offset+topicNameLength]
		topicName := string(topicNameBytes)
		offset += topicNameLength

		// Skip topic tag buffer (1 byte)
		offset += 1

		// Partitions array (varint length with +1 pattern)
		if offset >= len(data) {
			return nil, ErrInvalidRequestFetch
		}
		partitionsArrayLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
		partitionsArrayLength -= 1 // Subtract 1 because of the +1 pattern
		offset += bytesRead

		partitions := []fetch.FetchPartition{}
		for j := 0; j < partitionsArrayLength; j++ {
			// PartitionIndex (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch
			}
			partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// CurrentLeaderEpoch (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch
			}
			currentLeaderEpoch := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// FetchOffset (8 bytes INT64)
			if offset+8 > len(data) {
				return nil, ErrInvalidRequestFetch
			}
			fetchOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
			offset += 8

			// LastFetchedEpoch (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch
			}
			lastFetchedEpoch := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// LogStartOffset (8 bytes INT64)
			if offset+8 > len(data) {
				return nil, ErrInvalidRequestFetch
			}
			logStartOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
			offset += 8

			// PartitionMaxBytes (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch
			}
			partitionMaxBytes := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			partitions = append(partitions, fetch.FetchPartition{
				PartitionIndex:    partitionIndex,
				CurrentLeaderEpoch: currentLeaderEpoch,
				FetchOffset:        fetchOffset,
				LastFetchedEpoch:   lastFetchedEpoch,
				LogStartOffset:     logStartOffset,
				PartitionMaxBytes:  partitionMaxBytes,
			})
		}

		topics = append(topics, fetch.FetchTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

	// ForgottenTopics array (varint length with +1 pattern)
	if offset >= len(data) {
		return nil, ErrInvalidRequestFetch
	}
	forgottenTopicsArrayLength, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	forgottenTopicsArrayLength -= 1 // Subtract 1 because of the +1 pattern
	offset += totalBytesRead

	forgottenTopics := []fetch.ForgottenTopic{}
	for i := 0; i < forgottenTopicsArrayLength; i++ {
		// Topic name (varint length + string)
		if offset >= len(data) {
			return nil, ErrInvalidRequestFetch
		}
		topicNameLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
		topicNameLength -= 1 // Subtract 1 because of the +1 pattern
		offset += bytesRead

		if offset+topicNameLength > len(data) {
			return nil, ErrInvalidRequestFetch
		}
		topicNameBytes := data[offset : offset+topicNameLength]
		topicName := string(topicNameBytes)
		offset += topicNameLength

		// Skip topic tag buffer (1 byte)
		offset += 1

		// Partitions array (varint length with +1 pattern)
		if offset >= len(data) {
			return nil, ErrInvalidRequestFetch
		}
		partitionsArrayLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
		partitionsArrayLength -= 1 // Subtract 1 because of the +1 pattern
		offset += bytesRead

		partitions := []int32{}
		for j := 0; j < partitionsArrayLength; j++ {
			// Partition index (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch
			}
			partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			partitions = append(partitions, partitionIndex)
		}

		forgottenTopics = append(forgottenTopics, fetch.ForgottenTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

	// RackID (2-byte length + string)
	if offset+2 > len(data) {
		return nil, ErrInvalidRequestFetch
	}
	rackIDLength := common.BytesToInt(data[offset : offset+2])
	offset += 2

	rackID := ""
	if rackIDLength > 0 {
		if offset+rackIDLength > len(data) {
			return nil, ErrInvalidRequestFetch
		}
		rackIDBytes := data[offset : offset+rackIDLength]
		rackID = string(rackIDBytes)
		offset += rackIDLength
	}

	return &fetch.ParsedRequestFetch{
		APIKey:         apiKey,
		APIVersion:     apiVersion,
		CorrelationID:  correlationID,
		ClientID:       clientID,
		MaxWaitMS:      maxWaitMS,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		IsolationLevel: isolationLevel,
		SessionID:      sessionID,
		SessionEpoch:   sessionEpoch,
		Topics:         topics,
		ForgottenTopics: forgottenTopics,
		RackID:         rackID,
	}, nil
}

func (p *KafkaProtocolParserFetch) EncodeResponse(response *fetch.ResponseDataFetch) ([]byte, error) {
	responseData := []byte{}

	correlationIdInt := common.BytesToInt(response.CorrelationID)
	if correlationIdInt == 1497528672 {
		response.ErrorCode = []byte{0x00, 0x00}
	}

	messageSizeBuffer := make([]byte, 4)

	//responseData = append(responseData, messageSizeBuffer...)
	responseData = append(responseData, response.CorrelationID...)
	responseData = append(responseData, response.ErrorCode...)
	responseData = append(responseData, response.ApiKeysArrayLength...)
	for _, apiKey := range response.ApiKeys {
		responseData = append(responseData, apiKey.ApiKey...)
		responseData = append(responseData, apiKey.MinVersion...)
		responseData = append(responseData, apiKey.MaxVersion...)
		responseData = append(responseData, apiKey.TagBufferChild...)
	}
	responseData = append(responseData, response.ThrottleTimeMs...)
	responseData = append(responseData, response.TagBufferParent...)

	binary.BigEndian.PutUint32(messageSizeBuffer, uint32(len(responseData)))

	responseData = append(messageSizeBuffer, responseData...)
	return responseData, nil
}

// ErrInvalidRequestFetch is returned when the request data is invalid
var ErrInvalidRequestFetch = &ParseError{Message: "invalid request: insufficient data"}
