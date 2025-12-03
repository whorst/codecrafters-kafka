package application

import (
	"bytes"
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

	fmt.Printf("Received Request Data %+v\n", req.Data)

	var correlationId int32
	if err := binary.Read(bytes.NewReader(req.Data[4:8]), binary.BigEndian, &correlationId); err != nil {
		fmt.Println("Failed to read correlationId:", err)
		return domain.Response{}, fmt.Errorf("Could not read correlationId")
	}
	fmt.Println("Found correlation Id: ", correlationId)

	responseData := []byte{00, 00, 00, 00, req.Data[8], req.Data[9], req.Data[10], req.Data[11]}

	fmt.Printf("Response Data %+v", responseData)
	return domain.Response{
		Data: responseData,
	}, nil
}
