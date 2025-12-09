package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/core/application/kafka_describe_topic_service"
	"github.com/codecrafters-io/kafka-starter-go/core/application/kafka_router"
	"github.com/codecrafters-io/kafka-starter-go/core/application/kafka_service"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/driving"
	parser "github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/parser"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Create the parser adapters (protocol parser - infrastructure)
	protocolParser := parser.NewKafkaProtocolParser()
	kafkaService := kafka_service.NewKafkaService(protocolParser)

	protocolParserDescribeTopic := parser.NewKafkaProtocolParserDescribeTopic()
	kafkaServiceDescribeTopic := kafka_describe_topic_service.NewKafkaDescribeTopicService(protocolParserDescribeTopic)

	// Create unified router that routes based on API key
	router := kafka_router.NewKafkaRouter(kafkaService, kafkaServiceDescribeTopic)

	tcpServer := driving.NewTCPServer(router, "0.0.0.0:9092")

	// Start the server
	if err := tcpServer.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
}
