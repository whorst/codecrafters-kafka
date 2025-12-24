package fetch_repository

import (
	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	fetch_repo_port "github.com/codecrafters-io/kafka-starter-go/core/ports/repository/fetch"
)

type FetchRepository struct {
}

func NewFetchRepository() fetch_repo_port.FetchRepository {
	return &FetchRepository{}
}

func (f FetchRepository) GetTopicFetch(parsedReq domain.ParsedRequestFetch) (domain.ResponseDataFetch, error) {

	topics := []domain.FetchResponseTopic{}

	topics = append(topics, domain.FetchResponseTopic{
		Name: parsedReq.Topics[0].Name,
		Partitions: []domain.FetchResponsePartition{
			domain.FetchResponsePartition{
				PartitionIndex:       0,
				ErrorCode:            100,
				HighWatermark:        0,
				LastStableOffset:     0,
				LogStartOffset:       0,
				AbortedTransactions:  nil,
				PreferredReadReplica: 0,
				Records:              nil,
			},
		},
	})

	return domain.ResponseDataFetch{
		CorrelationID:  parsedReq.CorrelationID,
		ThrottleTimeMs: 0,      // Throttle time in milliseconds
		ErrorCode:      0,      // Error code (0 = no error)
		SessionID:      0,      // Session ID
		Topics:         topics, // Empty topics array for now
	}, nil
}
