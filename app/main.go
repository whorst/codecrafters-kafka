package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/core/application/api_version_service"
	"github.com/codecrafters-io/kafka-starter-go/core/application/fetch_service"
	"github.com/codecrafters-io/kafka-starter-go/core/application/kafka_describe_topic_service"
	"github.com/codecrafters-io/kafka-starter-go/core/application/kafka_router"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/driving"
	parser "github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/parser"
	cluster_metadata_adapter "github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/repository/cluster_metadata"
	fetch_repository "github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/repository/fetch"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Create the parser adapters (protocol parser - infrastructure)
	protocolParser := parser.NewKafkaProtocolParser()
	apiVersionService := api_version_service.NewApiVersionService(protocolParser)

	protocolParserDescribeTopic := parser.NewKafkaProtocolParserDescribeTopic()
	clusterMetadataParser := cluster_metadata_adapter.NewClusterMetadataParser()
	kafkaServiceDescribeTopic := kafka_describe_topic_service.NewKafkaDescribeTopicService(protocolParserDescribeTopic, clusterMetadataParser)

	protocolParserFetch := parser.NewKafkaProtocolParserFetch()
	fetchRepository := fetch_repository.NewFetchRepository()
	fetchService := fetch_service.NewFetchService(protocolParserFetch, fetchRepository)

	// Create unified router that routes based on API key
	router := kafka_router.NewKafkaRouter(apiVersionService, kafkaServiceDescribeTopic, fetchService)

	tcpServer := driving.NewTCPServer(router, "0.0.0.0:9092")

	// Start the server
	if err := tcpServer.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
}
