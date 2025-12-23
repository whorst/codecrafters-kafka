package parser

import (
	"github.com/codecrafters-io/kafka-starter-go/core/domain"
)

type FetchParser interface {
	// ParseRequest extracts structured data from raw binary request data
	ParseRequest(data []byte) (*domain.ParsedRequestFetch, error)

	// EncodeResponse converts a response into binary format
	EncodeResponse(response *domain.ResponseDataFetch) ([]byte, error)
}
