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
	
	// Extract the full RecordBatch first
	var extractedRecords []byte
	if recordBatchStart+recordBatchSize > len(data) {
		fmt.Printf("Warning: recordBatchSize (%d) exceeds data length (%d)\n", recordBatchSize, len(data))
		extractedRecords = data[recordBatchStart:]
	} else {
		extractedRecords = data[recordBatchStart : recordBatchStart+recordBatchSize]
	}
	
	// Parse the RecordBatch to find where the first record ends
	// RecordBatch structure:
	// - Base Offset (8 bytes)
	// - Batch Length (4 bytes)
	// - Partition Leader Epoch (4 bytes)
	// - Magic Byte (1 byte)
	// - CRC (4 bytes)
	// - Attributes (2 bytes)
	// - Last Offset Delta (4 bytes)
	// - Base Timestamp (8 bytes)
	// - Max Timestamp (8 bytes)
	// - Producer ID (8 bytes)
	// - Producer Epoch (2 bytes)
	// - Base Sequence (4 bytes)
	// - Records Length (4 bytes) - number of records
	// - Records (variable)
	
	offset := 0
	if len(extractedRecords) < 8+4 {
		partitionToFetch.TopicFetchResponse.Records = extractedRecords
		return
	}
	
	offset = 8 + 4 // Skip Base Offset and Batch Length
	
	// Skip RecordBatch header fields to get to Records Length
	headerSize := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 // All header fields before Records Length
	if offset+headerSize+4 > len(extractedRecords) {
		partitionToFetch.TopicFetchResponse.Records = extractedRecords
		return
	}
	
	offset += headerSize // Now at Records Length field
	
	// Read Records Length (number of records)
	recordsCount := common.BytesToInt(extractedRecords[offset : offset+4])
	offset += 4
	
	fmt.Printf(">>>>>>>> Records count: %d\n", recordsCount)
	
	if recordsCount <= 0 {
		partitionToFetch.TopicFetchResponse.Records = extractedRecords
		return
	}
	
	// Read the first record's Length (varint) - this tells us the total byte length of the record
	recordLength, bytesRead := common.ReadVarIntSigned(offset, extractedRecords)
	if bytesRead == 0 {
		partitionToFetch.TopicFetchResponse.Records = extractedRecords
		return
	}
	
	fmt.Printf(">>>>>>>> First record length (varint): %d, bytes read: %d\n", recordLength, bytesRead)
	
	// The record starts at 'offset', and its length is 'recordLength' bytes
	// So the record ends at offset + bytesRead + recordLength
	// But we need to include the record length varint itself, so:
	// Record data starts at offset + bytesRead
	// Record data ends at offset + bytesRead + recordLength
	// Total record size including length varint: bytesRead + recordLength
	
	firstRecordEndOffset := offset + bytesRead + recordLength
	
	fmt.Printf(">>>>>>>> First record starts at offset %d, ends at offset %d (total %d bytes including length varint)\n", 
		offset, firstRecordEndOffset, firstRecordEndOffset-offset)
	
	// Truncate the RecordBatch to only include up to the end of the first record
	// We need to keep the full RecordBatch header + Records Length + first record
	if firstRecordEndOffset > len(extractedRecords) {
		fmt.Printf("Warning: First record extends beyond extracted data, using full RecordBatch\n")
		partitionToFetch.TopicFetchResponse.Records = extractedRecords
	} else {
		partitionToFetch.TopicFetchResponse.Records = extractedRecords[:firstRecordEndOffset]
		fmt.Printf(">>>>>>>> Truncated RecordBatch from %d bytes to %d bytes (first record only)\n", 
			len(extractedRecords), len(partitionToFetch.TopicFetchResponse.Records))
	}
}
