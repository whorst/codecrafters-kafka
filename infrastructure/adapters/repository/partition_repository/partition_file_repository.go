package partition_file_repository

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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

	root := "/tmp/kraft-combined-logs/"

	// WalkDir is the most efficient way to recursively traverse directories in Go.
	// It visits every file and folder starting from the root path.
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		// If there is an error accessing a path (e.g., permissions), we report it
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accessing %s: %v\n", path, err)
			return nil // Return nil to continue walking other directories
		}

		// We only want to print files, not the directory names themselves
		fmt.Println(path)

		return nil
	})
	data, err := os.ReadFile(fileToFetch)
	if err != nil {
		fmt.Printf("Failed to get file: %v\n", err)
		panic("file partition error")
	}
	fmt.Println("Length of above file ", len(data))

	offset := 0

	// Read Base Offset (8 bytes)
	offset += 8

	// Read Batch Length (4 bytes) - this is the size of the RecordBatch excluding Base Offset and Batch Length
	batchLength := common.BytesToInt(data[offset : offset+4])
	offset += 4

	// The full RecordBatch size is: Base Offset (8) + Batch Length (4) + batchLength bytes
	recordBatchSize := 8 + 4 + batchLength

	// Read Records Length (number of records) for metadata
	recordsLengthOffset := 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 // Skip to recordsLength field
	if recordsLengthOffset+4 <= len(data) {
		recordsLength := common.BytesToInt(data[recordsLengthOffset : recordsLengthOffset+4])
		partitionToFetch.TopicFetchResponse.RecordsLength = recordsLength
	}

	// Extract the entire RecordBatch
	if recordBatchSize > len(data) {
		partitionToFetch.TopicFetchResponse.Records = data
	} else {
		partitionToFetch.TopicFetchResponse.Records = data[0:recordBatchSize]
	}
}
