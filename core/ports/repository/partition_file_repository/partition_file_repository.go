package partition_file_repository

import "github.com/codecrafters-io/kafka-starter-go/core/domain"

type PartitionFileRepository interface {
	GetPartitionMessage(messageFetchRequest domain.MessageFetchRequest)
}
