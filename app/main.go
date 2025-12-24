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
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/repository/cluster_metadata_repository"
	fetch_repository "github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/repository/fetch"
	partition_file_repository "github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/repository/partition_repository"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Create the parser adapters (protocol parser - infrastructure)
	protocolParser := parser.NewKafkaProtocolParser()
	apiVersionService := api_version_service.NewApiVersionService(protocolParser)

	protocolParserDescribeTopic := parser.NewKafkaProtocolParserDescribeTopic()
	clusterMetadataRepository := cluster_metadata_repository.NewClusterMetadataRepository()
	kafkaServiceDescribeTopic := kafka_describe_topic_service.NewKafkaDescribeTopicService(protocolParserDescribeTopic, clusterMetadataRepository)

	protocolParserFetch := parser.NewKafkaProtocolParserFetch()
	fetchRepository := fetch_repository.NewFetchRepository()
	partitionFileRepository := partition_file_repository.NewPartitionFileRepository()
	fetchService := fetch_service.NewFetchService(
		protocolParserFetch,
		fetchRepository,
		clusterMetadataRepository,
		partitionFileRepository)

	// Create unified router that routes based on API key
	router := kafka_router.NewKafkaRouter(apiVersionService, kafkaServiceDescribeTopic, fetchService)

	tcpServer := driving.NewTCPServer(router, "0.0.0.0:9092")

	// Start the server
	if err := tcpServer.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
}
