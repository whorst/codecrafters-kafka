package partition_file_repository

import (
	"fmt"
	"os"
	"strconv"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
)
import port_repo "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/partition_file_repository"

type PartitionFileRepository struct {
}

func NewPartitionFileRepository() port_repo.PartitionFileRepository {
	return &PartitionFileRepository{}
}

func (PartitionFileRepository) GetPartitionMessage(messageFetchRequest domain.MessageFetchRequest) []byte {
	for _, partitionToFetch := range messageFetchRequest.PartitionsToFetch {
		openLogFile(partitionToFetch)
	}
	return []byte{}
}

func openLogFile(partitionToFetch domain.PartitionToFetch) {

	fileToFetch := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%s/00000000000000000000", partitionToFetch.TopicName, strconv.Itoa(partitionToFetch.PartitionIndex))

	fmt.Println(">>>>>>>>> Attempting to fetch file", fileToFetch)

	data, err := os.ReadFile(fileToFetch)
	if err != nil {
		fmt.Printf("Failed to get file: %v\n", err)
		panic("file partition error")
	}
	fmt.Println(data)

	///tmp/kraft-combined-logs/bar-0/00000000000000000000
}
