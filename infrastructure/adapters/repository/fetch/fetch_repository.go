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

	if len(parsedReq.Topics) > 0 {
		frt := domain.FetchResponseTopic{
			TopicName:      parsedReq.Topics[0].Name,
			TopicNameBytes: parsedReq.Topics[0].TopicNameBytes,
			Partitions: []*domain.FetchResponsePartition{
				{
					PartitionIndex:       0,
					ErrorCode:            0,
					HighWatermark:        0,
					LastStableOffset:     0,
					LogStartOffset:       0,
					AbortedTransactions:  nil,
					PreferredReadReplica: 0,
					Records:              nil,
				},
			},
		}

		topics = append(topics, frt)
	}

	return domain.ResponseDataFetch{
		CorrelationID:  parsedReq.CorrelationID,
		ThrottleTimeMs: 0,      // Throttle time in milliseconds
		ErrorCode:      0,      // Error code (0 = no error)
		SessionID:      0,      // Session ID
		Topics:         topics, // Empty topics array for now
	}, nil
}
