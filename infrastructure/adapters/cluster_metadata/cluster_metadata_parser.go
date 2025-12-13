package cluster_metadata

import (
	"encoding/hex"
	"fmt"
	"os"

	clutser_metadata_port "github.com/codecrafters-io/kafka-starter-go/core/ports/cluster_metadata"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/partition_metadata"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/common"
)

type ClusterMetadata struct {
	*clutser_metadata_port.ClusterMetadataLogResponse
}

func NewClusterMetadataParser() *ClusterMetadata {
	return &ClusterMetadata{}
}

func (c *ClusterMetadata) ParseClusterMetadataFileByTopicNames() (error, clutser_metadata_port.ClusterMetadataLogResponse) {

	c.ClusterMetadataLogResponse = &clutser_metadata_port.ClusterMetadataLogResponse{}
	c.ClusterMetadataLogResponse.TopicUUIDTopicMetadataInfoMap = make(map[string]*clutser_metadata_port.TopicMetadataInfo)
	c.ClusterMetadataLogResponse.TopicUUIDPartitionMetadataMap = make(map[string][]*partition_metadata.PartitionMetadata)
	c.ClusterMetadataLogResponse.TopicNameTopicUuidMap = make(map[string]string)

	// 1. Parse a record batch
	// 2. Find the Records Array
	// 3. Figure out if the record in the record array is 1. A Topic Record, 2. A Partition Record, 3. A feature level record
	//	- If 1, Use the information in there to build the topic portion of the  DescribeTopicPartitions Response Body
	//  - If 2, Use the information to build the partitions array within the topic
	//	- If 3, ignore

	//Values to get from metadata
	/*
		error_code					INT16			Error code (0 for valid partitions)
		partition_index				INT32			The partition ID
		leader_id					INT32			The broker ID hosting this partition
		leader_epoch				INT32			The leader epoch
		replica_nodes				COMPACT_ARRAY	Array of replica broker IDs
		isr_nodes					COMPACT_ARRAY	Array of in-sync replica broker IDs
		eligible_leader_replicas	COMPACT_ARRAY	Array of eligible leader replica broker IDs
		last_known_elr				COMPACT_ARRAY	Array of last known eligible leader replicas
		offline_replicas			COMPACT_ARRAY	Array of offline replica broker IDs
		TAG_BUFFER					TAGGED_FIELDS	Tagged fields
	*/

	data, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return err, clutser_metadata_port.ClusterMetadataLogResponse{}
	}

	//data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4f, 0x00, 0x00, 0x00, 0x01, 0x02, 0xb0, 0x69, 0x45, 0x7c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x3a, 0x00, 0x00, 0x00, 0x01, 0x2e, 0x01, 0x0c, 0x00, 0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x01, 0x02, 0x24, 0xdb, 0x12, 0xdd, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x03, 0x3c, 0x00, 0x00, 0x00, 0x01, 0x30, 0x01, 0x02, 0x00, 0x04, 0x73, 0x61, 0x7a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91, 0x00, 0x00, 0x90, 0x01, 0x00, 0x00, 0x02, 0x01, 0x82, 0x01, 0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x90, 0x01, 0x00, 0x00, 0x04, 0x01, 0x82, 0x01, 0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00}
	c.processRecordBatches(data)

	return nil, *c.ClusterMetadataLogResponse
}

func (c *ClusterMetadata) processRecordBatches(data []byte) {
	currentOffset := 0
	for {
		if currentOffset >= len(data) {
			break
		}

		baseOffset := common.BytesToInt(data[currentOffset : currentOffset+8])
		currentOffset += 8
		_ = baseOffset

		batchLength := common.BytesToInt(data[currentOffset : currentOffset+4]) //batchLength will be the total bytes of a RecordBatch without the Base Offset and Batch Length
		currentOffset += 4
		_ = batchLength

		currentOffset = currentOffset + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4
		// Skipping the following values:
		// - Partition Leader Epoch
		//- Magic Byte
		//- CRC
		//- Attributes
		//- Last Offset Delta
		//- Base Timestamp
		//- Max Timestamp
		//- Producer ID
		//- Producer Epoch
		//- Base Sequence
		recordsLength := common.BytesToInt(data[currentOffset : currentOffset+4])
		currentOffset = currentOffset + 4

		allRecordsBytesRead := c.processRecords(data, currentOffset, recordsLength)
		currentOffset += allRecordsBytesRead
	}
}

func (c *ClusterMetadata) processRecords(data []byte, offset int, totalRecordsToProcess int) int {
	originalOffset := offset
	for range totalRecordsToProcess {
		totalRecordBytesRead := c.processRecord(data, offset)
		offset += totalRecordBytesRead
	}
	return offset - originalOffset
}

func (c *ClusterMetadata) processRecord(data []byte, offset int) int {
	originalOffset := offset

	recordLength, totalBytesRead := common.ReadVarIntSigned(offset, data)
	offset += totalBytesRead

	offset += 1 // Read Attributes

	timestampDelta, totalBytesRead := common.ReadVarIntSigned(offset, data)
	offset += totalBytesRead

	offsetDelta, totalBytesRead := common.ReadVarIntSigned(offset, data)
	offset += totalBytesRead

	keyLength, totalBytesRead := common.ReadVarIntSigned(offset, data)
	offset += totalBytesRead

	// TODO: This is Key. Is it always none? It's set to 0 according to the docs https://binspec.org/kafka-cluster-metadata?highlight=66-65

	valueLength, totalBytesRead := common.ReadVarIntSigned(offset, data)
	offset += totalBytesRead

	c.processRecordValue(data, offset)
	offset += valueLength

	_ = recordLength
	_ = timestampDelta
	_ = offsetDelta
	_ = keyLength
	_ = valueLength

	offset += 1 // Add one for headers array count
	allBytesRead := offset - originalOffset
	return allBytesRead
}

func (c *ClusterMetadata) processRecordValue(data []byte, offset int) {
	frameVersion := data[offset]
	offset += 1

	recordValueType := data[offset]
	offset += 1

	switch recordValueType {
	case 0x00:
		return
	case 0x01:
		// Feature Level Record, we ignore it
		return
	case 0x02:
		c.processTopicRecord(data, offset)
		// Topic Level Record
	case 0x03:
		c.processPartitionRecord(data, offset)
		return

	}
	_ = frameVersion
}

func (c *ClusterMetadata) processTopicRecord(data []byte, offset int) {
	version := data[offset]
	offset += 1

	nameLength, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	offset += totalBytesRead
	nameLength -= 1 // Because of https://binspec.org/kafka-cluster-metadata?highlight=161-161

	topicNameBytes := data[offset : offset+nameLength]
	topicNameString := string(topicNameBytes)
	fmt.Printf("Topic Name for Topic Record: %+v\n", topicNameString)
	offset += nameLength

	topicNameInfo := parser.TopicNameInfo{TopicNameBytes: topicNameBytes, TopicName: topicNameString}

	topicUuid := data[offset : offset+16]

	tmi := clutser_metadata_port.TopicMetadataInfo{
		TopicNameInfo:             topicNameInfo,
		TopicId:                   topicUuid,
		IsInternal:                []byte{0x00},
		PartitionsArray:           []*partition_metadata.PartitionMetadata{},
		TopicAuthorizedOperations: []byte{0x00, 0x00, 0x00, 0x00},
		TagBuffer:                 []byte{0x00},
	}

	c.TopicNameTopicUuidMap[topicNameString] = hex.EncodeToString(topicUuid)

	c.ClusterMetadataLogResponse.TopicUUIDTopicMetadataInfoMap[hex.EncodeToString(topicUuid)] = &tmi
	_ = version
}

func (c *ClusterMetadata) processPartitionRecord(data []byte, offset int) {
	version := data[offset]
	offset += 1
	_ = version

	partitionId := data[offset : offset+4]
	offset += 4
	_ = partitionId

	topicUuid := data[offset : offset+16]
	offset += 16
	_ = topicUuid

	partitionRecord := partition_metadata.PartitionMetadata{}
	c.TopicUUIDPartitionMetadataMap[hex.EncodeToString(topicUuid)] = append(c.TopicUUIDPartitionMetadataMap[hex.EncodeToString(topicUuid)], &partitionRecord)

	lengthOfReplicaArray, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	lengthOfReplicaArray -= 1 //https://binspec.org/kafka-cluster-metadata?highlight=214-214 subtract 1 because of here
	partitionRecord.ReplicaNodes.ArrayLength = data[offset : offset+totalBytesRead]
	offset += totalBytesRead

	for range lengthOfReplicaArray {
		replicaId := data[offset : offset+4] //TODO: This might be wrong https://binspec.org/kafka-cluster-metadata?highlight=289-292
		partitionRecord.ReplicaNodesArray = append(partitionRecord.ReplicaNodesArray, replicaId...)
		offset += 4
	}

	lengthOfInSyncReplicaArray, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	lengthOfInSyncReplicaArray -= 1 //https://binspec.org/kafka-cluster-metadata?highlight=219-219 subtract 1 because of here
	partitionRecord.IsrNodes.ArrayLength = data[offset : offset+totalBytesRead]
	offset += totalBytesRead

	for range lengthOfInSyncReplicaArray {
		inSyncReplicaArray := data[offset : offset+4] // TODO: this might be wrong https://binspec.org/kafka-cluster-metadata?highlight=220-223
		partitionRecord.IsrNodes.IsrNodeArray = append(partitionRecord.IsrNodes.IsrNodeArray, inSyncReplicaArray...)

		offset += 4

	}

	lengthOfRemovingReplicasArray, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	lengthOfRemovingReplicasArray -= 1 //https://binspec.org/kafka-cluster-metadata?highlight=224-224 subtract 1 because of here
	offset += totalBytesRead

	lengthOfAddingReplicasArray, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	lengthOfAddingReplicasArray -= 1 //https://binspec.org/kafka-cluster-metadata?highlight=225-225 subtract 1 because of here
	offset += totalBytesRead

	replicaIdOfLeader := data[offset : offset+4]
	_ = replicaIdOfLeader
	offset += 4

	epochOfLeader := data[offset : offset+4]
	_ = epochOfLeader
	offset += 4

	epochOfPartition := data[offset : offset+4]
	_ = epochOfPartition
	offset += 4

	lengthOfDirectoriesArray, totalBytesRead := common.ReadVarIntUnsigned(offset, data)
	_ = lengthOfDirectoriesArray
	offset += totalBytesRead

	directoriesArrayUuid := data[offset : offset+16] //https://binspec.org/kafka-cluster-metadata?highlight=239-254
	_ = directoriesArrayUuid
	offset += 16

	offset += 1 //Skip tagged fields count https://binspec.org/kafka-cluster-metadata?highlight=255-255

	partitionRecord.ErrorCode = []byte{0x00, 0x00}
	topicPartitionMetadataSlice := c.TopicUUIDPartitionMetadataMap[hex.EncodeToString(topicUuid)]
	partitionRecord.PartitionIndex = common.IntToFourBytes(len(topicPartitionMetadataSlice) - 1)
	partitionRecord.LeaderId = replicaIdOfLeader //TODO: Is this correct? https://binspec.org/kafka-cluster-metadata?highlight=300-303 but the expectation is https://binspec.org/kafka-describe-topic-partitions-response-v0?highlight=44-47
	partitionRecord.LeaderEpoch = epochOfLeader

	partitionRecord.EligibleLeaderReplicasArrayLength = []byte{0x01} //TODO: is this correct? https://binspec.org/kafka-describe-topic-partitions-response-v0?highlight=62-62
	partitionRecord.LastKnownElrArrayLength = []byte{0x01}           //TODO: Same https://binspec.org/kafka-describe-topic-partitions-response-v0?highlight=62-62
	partitionRecord.OfflineReplicasArrayLength = []byte{0x01}
	partitionRecord.TagBuffer = []byte{0x00}
}
