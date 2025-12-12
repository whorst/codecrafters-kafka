package clutser_metadata

import (
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/partition_metadata"
)

type ClusterMetadataParser interface {
	ParseClusterMetadataFileByTopicNames(topicNames []string) (error, ClusterMetadataLogResponse)
}

type ClusterMetadataLogResponse struct {
	TopicUUIDTopicMetadataInfoMap map[string]*TopicMetadataInfo
	TopicUUIDPartitionMetadataMap map[string][]*partition_metadata.PartitionMetadata
}

type TopicMetadataInfo struct {
	TopicNameInfo             parser.TopicNameInfo // From the request?          // x
	TopicId                   []byte               // UUID                       // x
	IsInternal                []byte               // 1 byte, hard coded to 00   // x
	PartitionsArray           []*partition_metadata.PartitionMetadata
	TopicAuthorizedOperations []byte // x
	TagBuffer                 []byte //A single byte value 0x00                  // x

}
