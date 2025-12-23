package fetch

import "github.com/codecrafters-io/kafka-starter-go/core/domain"

type FetchRepository interface {
	GetTopicFetch(parsedReq domain.ParsedRequestFetch) (domain.ResponseDataFetch, error)
}
