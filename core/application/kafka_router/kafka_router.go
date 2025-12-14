package kafka_router

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
)

// KafkaRouter is a unified handler that routes requests to the appropriate service
// based on the API key in the request.
type KafkaRouter struct {
	apiVersionsHandler   driving.KafkaHandler
	describeTopicHandler driving.KafkaHandler
	fetchHandler         driving.KafkaHandler
}

// NewKafkaRouter creates a new Kafka router that implements the driving port
func NewKafkaRouter(apiVersionsHandler, describeTopicHandler driving.KafkaHandler, fetchHandler driving.KafkaHandler) driving.KafkaHandler {
	return &KafkaRouter{
		apiVersionsHandler:   apiVersionsHandler,
		describeTopicHandler: describeTopicHandler,
		fetchHandler:         fetchHandler,
	}
}

// HandleRequest routes the request to the appropriate handler based on the API key.
// The API key is located at bytes 4-5 in the Kafka request (after the 4-byte request size).
func (r *KafkaRouter) HandleRequest(req domain.Request) (domain.Response, error) {
	// Extract API key from request
	// Kafka request format: [4 bytes size][2 bytes API key][2 bytes API version][...]
	if len(req.Data) < 6 {
		// If request is too short, default to API Versions handler
		return r.apiVersionsHandler.HandleRequest(req)
	}

	// API key is at bytes 4-5 (0-indexed: indices 4 and 5)
	apiKeyBytes := req.Data[4:6]
	apiKey := binary.BigEndian.Uint16(apiKeyBytes)

	// Route based on API key
	// API Key 1 == Fetch
	// API key 18 (0x00, 0x12) = ApiVersions
	// API key 75 (0x4B) = DescribeTopicPartitions
	switch apiKey {
	case 1:
		return r.fetchHandler.HandleRequest(req)
	case 18: // ApiVersions
		return r.apiVersionsHandler.HandleRequest(req)
	case 75: // DescribeTopicPartitions
		return r.describeTopicHandler.HandleRequest(req)
	default:
		// Default to API Versions handler for unknown API keys
		return r.apiVersionsHandler.HandleRequest(req)
	}
}
