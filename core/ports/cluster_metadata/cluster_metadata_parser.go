package clutser_metadata

import "github.com/codecrafters-io/kafka-starter-go/core/ports/parser"

type ClusterMetadataParser interface {
	ParseClusterMetadataFileByTopicNames(topicNames []string) (error, ClusterMetadataLogResponse)
}

type ClusterMetadataLogResponse struct {
	TopicUUIDTopicMetadataInfoMap map[string]*TopicMetadataInfo
	TopicUUIDPartitionMetadataMap map[string][]*PartitionMetadata
}

type TopicMetadataInfo struct {
	TopicNameInfo             parser.TopicNameInfo // From the request?          // x
	TopicId                   []byte               // UUID                       // x
	IsInternal                []byte               // 1 byte, hard coded to 00   // x
	PartitionsArray           []*PartitionMetadata
	TopicAuthorizedOperations []byte // x
	TagBuffer                 []byte //A single byte value 0x00                  // x

}

type PartitionMetadata struct {
	ErrorCode                         []byte // 2 bytes, set to 0                                 x
	PartitionIndex                    []byte // The index of the partition in PartitionsArray     x
	LeaderId                          []byte // 4 byte id of the leader for this partition        x
	LeaderEpoch                       []byte // 4 byte representing the epoch of the leader       x
	ReplicaNodes                             //												   x
	IsrNodes                                 //                                                   x
	EligibleLeaderReplicasArrayLength []byte //  							   x
	LastKnownElrArrayLength           []byte //  							   x
	OfflineReplicasArrayLength        []byte //  							   x
	TagBuffer                         []byte //A single byte value 0x00    //  x
}

type ReplicaNodes struct {
	ArrayLength       []byte
	ReplicaNodesArray []byte // Each one is 4 bytes, and is the Replica ID https://binspec.org/kafka-describe-topic-partitions-response-v0?highlight=52-56
}

type IsrNodes struct {
	ArrayLength  []byte
	IsrNodeArray []byte // Each one is 4 bytes, and is the in sync Replica node ID https://binspec.org/kafka-describe-topic-partitions-response-v0?highlight=57-61
}

// Record Batch #2, Record #2 offset is correct, but it's not parsed correctly. Starting @offset 183
