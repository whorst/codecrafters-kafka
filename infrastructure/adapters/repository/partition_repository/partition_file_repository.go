package partition_file_repository

import (
	"fmt"
	"os"
	"strconv"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/infrastructure/common"
)
import port_repo "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/partition_file_repository"

type PartitionFileRepository struct {
}

func NewPartitionFileRepository() port_repo.PartitionFileRepository {
	return &PartitionFileRepository{}
}

func (PartitionFileRepository) GetPartitionMessage(messageFetchRequest domain.MessageFetchRequest) {
	for _, partitionToFetch := range messageFetchRequest.PartitionsToFetch {
		openLogFile(partitionToFetch)
	}
}

func openLogFile(partitionToFetch domain.PartitionToFetch) {

	fileToFetch := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%s/00000000000000000000.log", partitionToFetch.TopicName, strconv.Itoa(partitionToFetch.PartitionIndex))

	data, err := os.ReadFile(fileToFetch)
	if err != nil {
		fmt.Printf("Failed to get file: %v\n", err)
		panic("file partition error")
	}

	// Calculate offset to Records Length field
	// Base Offset (8) + Batch Length (4) + Partition Leader Epoch (4) + Magic (1) + CRC (4) + 
	// Attributes (2) + Last Offset Delta (4) + Base Timestamp (8) + Max Timestamp (8) + 
	// Producer ID (8) + Producer Epoch (2) + Base Sequence (4) = 57 bytes
	recordsLengthOffset := 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4

	// Read Records Length (number of records) - 4 bytes
	if recordsLengthOffset+4 <= len(data) {
		recordsLength := common.BytesToInt(data[recordsLengthOffset : recordsLengthOffset+4])
		partitionToFetch.TopicFetchResponse.RecordsLength = recordsLength
	}

	// Read Batch Length to determine the full RecordBatch size
	// Batch Length is at offset 8 (after Base Offset)
	if 8+4 <= len(data) {
		batchLength := common.BytesToInt(data[8 : 8+4])
		// Full RecordBatch size = Base Offset (8) + Batch Length (4) + batchLength bytes
		recordBatchSize := 8 + 4 + batchLength
		
		// Set Records to the full RecordBatch starting from the beginning
		if recordBatchSize <= len(data) {
			partitionToFetch.TopicFetchResponse.Records = data[0:recordBatchSize]
		} else {
			partitionToFetch.TopicFetchResponse.Records = data
		}
	} else {
		partitionToFetch.TopicFetchResponse.Records = data
	}

}
