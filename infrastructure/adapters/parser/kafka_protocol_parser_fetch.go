package parser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/common"
)

type KafkaProtocolParserFetch struct{}

// NewKafkaProtocolParser creates a new Kafka protocol parser
func NewKafkaProtocolParserFetch() *KafkaProtocolParserFetch {
	return &KafkaProtocolParserFetch{}
}

func (p *KafkaProtocolParserFetch) ParseRequest(data []byte) (*domain.ParsedRequestFetch, error) {
	// Skip the 4-byte message size at the beginning
	if len(data) < 16 {
		return nil, ErrInvalidRequestFetch("message header")
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
		return nil, ErrInvalidRequestFetch("ClientID length")
	}
	clientIDLength := common.BytesToInt(data[offset : offset+2])
	offset += 2

	if offset+clientIDLength > len(data) {
		return nil, ErrInvalidRequestFetch("ClientID")
	}
	clientIDBytes := data[offset : offset+clientIDLength]
	clientID := string(clientIDBytes)
	offset += clientIDLength

	// Skip tag buffer (1 byte) after ClientID
	offset += 1

	// Parse Body
	// MaxWaitMS (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch("MaxWaitMS")
	}
	maxWaitMS := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// MinBytes (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch("MinBytes")
	}
	minBytes := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// MaxBytes (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch("MaxBytes")
	}
	maxBytes := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// IsolationLevel (1 byte)
	if offset+1 > len(data) {
		return nil, ErrInvalidRequestFetch("IsolationLevel")
	}
	isolationLevel := int8(data[offset])
	offset += 1

	// SessionID (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch("SessionID")
	}

	sessionID := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// SessionEpoch (4 bytes INT32)
	if offset+4 > len(data) {
		return nil, ErrInvalidRequestFetch("SessionEpoch")
	}
	sessionEpoch := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Topics array (varint length with +1 pattern)
	if offset >= len(data) {
		return nil, ErrInvalidRequestFetch("Topics array length")
	}
	topicsArrayLength, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	topicsArrayLength -= 1 // Subtract 1 because of the +1 pattern
	offset += totalBytesRead

	topics := []domain.FetchTopic{}
	for i := 0; i < topicsArrayLength; i++ {
		// Topic name (UUID - 16 bytes fixed length)
		// In Kafka protocol v16, topic names can be UUIDs encoded as 16 bytes
		if offset+16 > len(data) {
			return nil, ErrInvalidRequestFetch("Topic name UUID")
		}
		topicNameBytes := data[offset : offset+16]
		// Format UUID as string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
		topicName := fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
			topicNameBytes[0], topicNameBytes[1], topicNameBytes[2], topicNameBytes[3],
			topicNameBytes[4], topicNameBytes[5],
			topicNameBytes[6], topicNameBytes[7],
			topicNameBytes[8], topicNameBytes[9],
			topicNameBytes[10], topicNameBytes[11], topicNameBytes[12], topicNameBytes[13], topicNameBytes[14], topicNameBytes[15])
		offset += 16

		// Skip topic tag buffer (1 byte)
		offset += 1

		// Partitions array (varint length with +1 pattern)
		if offset >= len(data) {
			return nil, ErrInvalidRequestFetch("Partitions array length")
		}
		partitionsArrayLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
		partitionsArrayLength -= 1 // Subtract 1 because of the +1 pattern
		offset += bytesRead

		partitions := []domain.FetchPartition{}
		for j := 0; j < partitionsArrayLength; j++ {
			// PartitionIndex (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch("PartitionIndex")
			}
			partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// CurrentLeaderEpoch (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch("CurrentLeaderEpoch")
			}
			currentLeaderEpoch := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// FetchOffset (8 bytes INT64)
			if offset+8 > len(data) {
				return nil, ErrInvalidRequestFetch("FetchOffset")
			}
			fetchOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
			offset += 8

			// LastFetchedEpoch (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch("LastFetchedEpoch")
			}
			lastFetchedEpoch := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// LogStartOffset (8 bytes INT64)
			if offset+8 > len(data) {
				return nil, ErrInvalidRequestFetch("LogStartOffset")
			}
			logStartOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
			offset += 8

			// PartitionMaxBytes (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch("PartitionMaxBytes")
			}
			partitionMaxBytes := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			partitions = append(partitions, domain.FetchPartition{
				PartitionIndex:     partitionIndex,
				CurrentLeaderEpoch: currentLeaderEpoch,
				FetchOffset:        fetchOffset,
				LastFetchedEpoch:   lastFetchedEpoch,
				LogStartOffset:     logStartOffset,
				PartitionMaxBytes:  partitionMaxBytes,
			})
		}

		topics = append(topics, domain.FetchTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

	// ForgottenTopics array (varint length with +1 pattern)
	if offset >= len(data) {
		return nil, ErrInvalidRequestFetch("ForgottenTopics array length")
	}
	forgottenTopicsArrayLength, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	forgottenTopicsArrayLength -= 1 // Subtract 1 because of the +1 pattern
	offset += totalBytesRead

	forgottenTopics := []domain.ForgottenTopic{}
	for i := 0; i < forgottenTopicsArrayLength; i++ {
		// Topic name (varint length + string)
		if offset >= len(data) {
			return nil, ErrInvalidRequestFetch("ForgottenTopic name length")
		}
		topicNameLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
		topicNameLength -= 1 // Subtract 1 because of the +1 pattern
		offset += bytesRead

		if offset+topicNameLength > len(data) {
			return nil, ErrInvalidRequestFetch("ForgottenTopic name")
		}
		topicNameBytes := data[offset : offset+topicNameLength]
		topicName := string(topicNameBytes)
		offset += topicNameLength

		// Skip topic tag buffer (1 byte)
		offset += 1

		// Partitions array (varint length with +1 pattern)
		if offset >= len(data) {
			return nil, ErrInvalidRequestFetch("ForgottenTopic partitions array length")
		}
		partitionsArrayLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
		partitionsArrayLength -= 1 // Subtract 1 because of the +1 pattern
		offset += bytesRead

		partitions := []int32{}
		for j := 0; j < partitionsArrayLength; j++ {
			// Partition index (4 bytes INT32)
			if offset+4 > len(data) {
				return nil, ErrInvalidRequestFetch("ForgottenTopic partition index")
			}
			partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			partitions = append(partitions, partitionIndex)
		}

		forgottenTopics = append(forgottenTopics, domain.ForgottenTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

	// RackID (varint length + string, with +1 pattern)
	if offset >= len(data) {
		return nil, ErrInvalidRequestFetch("RackID length")
	}
	rackIDLength, bytesRead := common.ReadVarIntUnsigned(offset, data)
	rackIDLength -= 1 // Subtract 1 because of the +1 pattern
	offset += bytesRead

	rackID := ""
	if rackIDLength > 0 {
		if offset+rackIDLength > len(data) {
			return nil, ErrInvalidRequestFetch("RackID")
		}
		rackIDBytes := data[offset : offset+rackIDLength]
		rackID = string(rackIDBytes)
		offset += rackIDLength
	}

	return &domain.ParsedRequestFetch{
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

func (p *KafkaProtocolParserFetch) printString(all []byte) string {
	return hex.EncodeToString(all)
}

func (p *KafkaProtocolParserFetch) EncodeResponse(response *domain.ResponseDataFetch) ([]byte, error) {
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

	// Fake to make tests pass
	//responseData = append(responseData, 0x00)

	// Topics array (varint length with +1 pattern)
	topicsLength := len(response.Topics) + 1
	topicsLengthVarInt := common.IntToVarInt(topicsLength)
	responseData = append(responseData, topicsLengthVarInt...)

	// Encode each topic
	for _, topic := range response.Topics {
		// Topic name (UUID - 16 bytes fixed length, no varint length prefix)
		// Parse UUID string back to 16 bytes: remove dashes and convert hex to bytes
		uuidHex := topic.Name
		// Remove dashes if present
		uuidHex = strings.ReplaceAll(uuidHex, "-", "")
		// Convert hex string to bytes
		uuidBytes, err := hex.DecodeString(uuidHex)
		if err != nil || len(uuidBytes) != 16 {
			return nil, fmt.Errorf("invalid UUID format: %s", topic.Name)
		}
		responseData = append(responseData, uuidBytes...)

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

// ErrInvalidRequestFetch returns a parse error with the specified field name
func ErrInvalidRequestFetch(fieldName string) *ParseError {
	return &ParseError{
		Message: fmt.Sprintf("invalid request: insufficient data for field '%s'", fieldName),
	}
}
