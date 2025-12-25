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
	_ = batchLength

	currentOffset = currentOffset + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4
	recordsLength := common.BytesToInt(data[currentOffset : currentOffset+4])
	currentOffset = currentOffset + 4
	partitionToFetch.TopicFetchResponse.RecordsLength = recordsLength

	fmt.Println(">>>>>>>> showing record stuff", len(data), currentOffset)
	partitionToFetch.TopicFetchResponse.Records = data[currentOffset:]
}
