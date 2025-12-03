package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/core/application"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/driven"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/driving"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Composition Root: Wire up the hexagonal architecture
	// Following the three immutable rules:
	// 1. Core defines the ports (core/ports/driving/KafkaHandler, core/ports/parser/ProtocolParser)
	// 2. Application implements driving port, uses parser port (core/application/KafkaService)
	// 3. Adapters implement/use ports (infrastructure/adapters/*)
	//    Dependencies point inward: infrastructure -> core

	// Create the parser adapter (protocol parser - infrastructure)
	protocolParser := parser.NewKafkaProtocolParser()

	// Create the application service (core business logic that implements the driving port)
	// It uses the parser port (protocol parser) for parsing/encoding
	kafkaService := application.NewKafkaService(protocolParser)

	// Create the primary adapter (driving adapter that uses the driving port)
	tcpServer := driving.NewTCPServer(kafkaService, "0.0.0.0:9092")

	// Start the server
	if err := tcpServer.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
}
