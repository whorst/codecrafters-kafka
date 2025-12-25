package cluster_metadata_repository

import (
	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
)

type ClusterMetadataRepository interface {
	GetClusterMetadata() (ClusterMetadataRepositoryResponse, error)
}

type ClusterMetadataRepositoryResponse struct {
	TopicUUIDTopicMetadataInfoMap map[string]*TopicMetadataInfo
	TopicUUIDPartitionMetadataMap map[string][]*domain.PartitionMetadata
	TopicNameTopicUuidMap         map[string]string
	RecordsLength                 int
	StartingRecordOffset          int
}

type TopicMetadataInfo struct {
	TopicNameInfo             parser.TopicNameInfo // From the request?          // x
	TopicId                   []byte               // UUID                       // x
	IsInternal                []byte               // 1 byte, hard coded to 00   // x
	PartitionsArray           []*domain.PartitionMetadata
	TopicAuthorizedOperations []byte // x
	TagBuffer                 []byte //A single byte value 0x00                  // x

}
