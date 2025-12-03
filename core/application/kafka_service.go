package application

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/driving"
)

// KafkaService implements the driving port (KafkaHandler interface).
// This is the application core that contains the business logic.
// Rule 2: The application implements the port defined by the core.
type KafkaService struct{}

// NewKafkaService creates a new Kafka service that implements the driving port
func NewKafkaService() driving.KafkaHandler {
	return &KafkaService{}
}

// HandleRequest processes a Kafka request and returns a response.
// This is where the core business logic lives.
func (s *KafkaService) HandleRequest(req domain.Request) (domain.Response, error) {
	// TODO: Implement actual Kafka protocol parsing and handling
	// For now, return the hardcoded response from the original code

	errorCodeBuffer := make([]byte, 4)

	fmt.Printf("Received Request Data %+v\n", req.Data)
	correlationIdBytes := getCorrelationIdFromMessageHeader(req.Data)
	requestApiVersionBytes := getRequestApiVersionFromMessageHeader(req.Data)
	apiVersion := parseBytesToInt(requestApiVersionBytes)
	fmt.Printf("Received API Version %v\n", apiVersion)
	if apiVersion < 0 || apiVersion > 4 {
		binary.BigEndian.PutUint32(errorCodeBuffer, uint32(35))
	}

	responseData := []byte{00, 00, 00, 00}
	responseData = append(responseData, correlationIdBytes...)
	responseData = append(responseData, errorCodeBuffer...)

	fmt.Printf("Response Data %+v", responseData)
	return domain.Response{
		Data: responseData,
	}, nil
}
