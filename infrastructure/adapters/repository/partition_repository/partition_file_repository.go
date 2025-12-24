package partition_file_repository

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
		if !d.IsDir() {
			fmt.Println(path)
		}

		return nil
	})

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
