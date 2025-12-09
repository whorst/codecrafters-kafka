package cluster_metadata

import (
	"fmt"
	"os"

	clutser_metadata_port "github.com/codecrafters-io/kafka-starter-go/core/ports/cluster_metadata"
)

type ClusterMetadata struct{}

func NewClusterMetadataParser() ClusterMetadata {
	return ClusterMetadata{}
}

func (c ClusterMetadata) ParseClusterMetadataFileByTopicNames(topicNames []string) (error, clutser_metadata_port.ClusterMetadataLogResponse) {

	data, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return err, clutser_metadata_port.ClusterMetadataLogResponse{}
	}

	fmt.Printf("2. Raw Bytes (Hexadecimal, continuous block):\n%X\n\n", data)

	return nil, clutser_metadata_port.ClusterMetadataLogResponse{}
}
