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

	fmt.Println(">>>>>>>>> Attempting to fetch file", fileToFetch)

	data, err := os.ReadFile(fileToFetch)
	if err != nil {
		fmt.Printf("Failed to get file: %v\n", err)
		panic("file partition error")
	}
	fmt.Println("Length of above file ", len(data))

	//fullyProcessed := cluster_metadata_repository.ProcessRecordBatchesPublic(data)

	currentOffset := 0

	baseOffset := common.BytesToInt(data[currentOffset : currentOffset+8])
	currentOffset += 8
	_ = baseOffset

	batchLength := common.BytesToInt(data[currentOffset : currentOffset+4]) //batchLength will be the total bytes of a RecordBatch without the Base Offset and Batch Length
	currentOffset += 4
	
	// The Records field should contain the full RecordBatch (starting from baseOffset)
	// batchLength is the size of the RecordBatch excluding Base Offset and Batch Length
	// So the full RecordBatch is: Base Offset (8) + Batch Length (4) + batchLength bytes
	recordBatchStart := 0 // Start from baseOffset
	recordBatchSize := 8 + 4 + batchLength // Base Offset + Batch Length + batchLength bytes
	
	// Read the recordsLength (number of records) for encoding
	recordsLengthOffset := 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 // Skip to recordsLength field
	if recordsLengthOffset+4 <= len(data) {
		recordsLength := common.BytesToInt(data[recordsLengthOffset : recordsLengthOffset+4])
		partitionToFetch.TopicFetchResponse.RecordsLength = recordsLength
	} else {
		partitionToFetch.TopicFetchResponse.RecordsLength = 0
	}
	
	fmt.Println(">>>>>>>> showing record stuff", len(data), "batchLength:", batchLength, "recordBatchSize:", recordBatchSize, "recordsLength:", partitionToFetch.TopicFetchResponse.RecordsLength)
	
	// Extract the full RecordBatch (Base Offset + Batch Length + batch data)
	if recordBatchStart+recordBatchSize > len(data) {
		fmt.Printf("Warning: recordBatchSize (%d) exceeds data length (%d)\n", recordBatchSize, len(data))
		partitionToFetch.TopicFetchResponse.Records = data[recordBatchStart:]
	} else {
		partitionToFetch.TopicFetchResponse.Records = data[recordBatchStart : recordBatchStart+recordBatchSize]
	}
}
