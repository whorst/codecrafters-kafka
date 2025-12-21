package parser

import (
	"encoding/binary"
	"fmt"

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
				PartitionIndex:     partitionIndex,
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

	// RackID (varint length + string, with +1 pattern)
	if offset >= len(data) {
		return nil, ErrInvalidRequestFetch
	}
	rackIDLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
	rackIDLength -= 1 // Subtract 1 because of the +1 pattern
	offset += bytesRead

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
		APIKey:          apiKey,
		APIVersion:      apiVersion,
		CorrelationID:   correlationID,
		ClientID:        clientID,
		MaxWaitMS:       maxWaitMS,
		MinBytes:        minBytes,
		MaxBytes:        maxBytes,
		IsolationLevel:  isolationLevel,
		SessionID:       sessionID,
		SessionEpoch:    sessionEpoch,
		Topics:          topics,
		ForgottenTopics: forgottenTopics,
		RackID:          rackID,
	}, nil
}

func (p *KafkaProtocolParserFetch) EncodeResponse(response *fetch.ResponseDataFetch) ([]byte, error) {
	responseData := []byte{}

	// CorrelationID (4 bytes)
	responseData = append(responseData, response.CorrelationID...)

	// ThrottleTimeMs (4 bytes INT32)
	throttleTimeMsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTimeMsBytes, uint32(response.ThrottleTimeMs))
	responseData = append(responseData, throttleTimeMsBytes...)

	// ErrorCode (2 bytes INT16)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	responseData = append(responseData, errorCodeBytes...)

	// SessionID (4 bytes INT32)
	sessionIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sessionIDBytes, uint32(response.SessionID))
	responseData = append(responseData, sessionIDBytes...)

	// Topics array (varint length with +1 pattern)
	topicsLength := len(response.Topics) + 1
	fmt.Println(">>>>>>>>>>>>> ", topicsLength)
	topicsLengthVarInt := common.IntToFourBytes(topicsLength)
	responseData = append(responseData, topicsLengthVarInt...)

	// Encode each topic
	for _, topic := range response.Topics {
		// Topic name (varint length + string, with +1 pattern)
		topicNameLength := len(topic.Name) + 1
		topicNameLengthVarInt := common.IntToVarInt(topicNameLength)
		responseData = append(responseData, topicNameLengthVarInt...)
		responseData = append(responseData, []byte(topic.Name)...)

		// Topic tag buffer (1 byte)
		responseData = append(responseData, 0x00)

		// Partitions array (varint length with +1 pattern)
		partitionsLength := len(topic.Partitions) + 1
		partitionsLengthVarInt := common.IntToVarInt(partitionsLength)
		responseData = append(responseData, partitionsLengthVarInt...)

		// Encode each partition
		for _, partition := range topic.Partitions {
			// PartitionIndex (4 bytes INT32)
			partitionIndexBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIndexBytes, uint32(partition.PartitionIndex))
			responseData = append(responseData, partitionIndexBytes...)

			// ErrorCode (2 bytes INT16)
			errorCodeBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(errorCodeBytes, uint16(partition.ErrorCode))
			responseData = append(responseData, errorCodeBytes...)

			// HighWatermark (8 bytes INT64)
			highWatermarkBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(highWatermarkBytes, uint64(partition.HighWatermark))
			responseData = append(responseData, highWatermarkBytes...)

			// LastStableOffset (8 bytes INT64)
			lastStableOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(lastStableOffsetBytes, uint64(partition.LastStableOffset))
			responseData = append(responseData, lastStableOffsetBytes...)

			// LogStartOffset (8 bytes INT64)
			logStartOffsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(logStartOffsetBytes, uint64(partition.LogStartOffset))
			responseData = append(responseData, logStartOffsetBytes...)

			// AbortedTransactions array (varint length with +1 pattern)
			abortedTxLength := len(partition.AbortedTransactions) + 1
			abortedTxLengthVarInt := common.IntToVarInt(abortedTxLength)
			responseData = append(responseData, abortedTxLengthVarInt...)

			// Encode each aborted transaction
			for _, abortedTx := range partition.AbortedTransactions {
				// ProducerID (8 bytes INT64)
				producerIDBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(producerIDBytes, uint64(abortedTx.ProducerID))
				responseData = append(responseData, producerIDBytes...)

				// FirstOffset (8 bytes INT64)
				firstOffsetBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(firstOffsetBytes, uint64(abortedTx.FirstOffset))
				responseData = append(responseData, firstOffsetBytes...)
			}

			// PreferredReadReplica (4 bytes INT32)
			preferredReadReplicaBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(preferredReadReplicaBytes, uint32(partition.PreferredReadReplica))
			responseData = append(responseData, preferredReadReplicaBytes...)

			// Records (variable length - just append the bytes)
			responseData = append(responseData, partition.Records...)
		}
	}

	// Tag buffer after Topics array (1 byte)
	responseData = append(responseData, 0x00)

	// Prepend message size (4 bytes)
	messageSizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(messageSizeBuffer, uint32(len(responseData)))
	responseData = append(messageSizeBuffer, responseData...)

	return responseData, nil
}

// ErrInvalidRequestFetch is returned when the request data is invalid
var ErrInvalidRequestFetch = &ParseError{Message: "invalid request: insufficient data"}
