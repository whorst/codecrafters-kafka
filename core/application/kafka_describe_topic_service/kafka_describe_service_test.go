package kafka_describe_topic_service

import (
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	portparser "github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
	infraparser "github.com/codecrafters-io/kafka-starter-go/infrastructure/adapters/parser"
)

// mockParser is a mock implementation of ProtocolParser for testing
type mockParser struct {
	parseRequestFunc   func(data []byte) (*portparser.ParsedRequestDescribeTopic, error)
	encodeResponseFunc func(response *portparser.ResponseDataDescribeTopic) ([]byte, error)
}

func (m *mockParser) ParseRequest(data []byte) (*portparser.ParsedRequestDescribeTopic, error) {
	if m.parseRequestFunc != nil {
		return m.parseRequestFunc(data)
	}
	return nil, nil
}

func (m *mockParser) EncodeResponse(response *portparser.ResponseDataDescribeTopic) ([]byte, error) {
	if m.encodeResponseFunc != nil {
		return m.encodeResponseFunc(response)
	}
	// Default: just return the error code bytes for testing
	return response.CorrelationID, nil
}

func TestKafkaKafkaDescribeService(t *testing.T) {
	tests := []struct {
		data []byte
		name string
	}{
		{data: []byte{}, name: "Foo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := infraparser.NewKafkaProtocolParserDescribeTopic()
			service := NewKafkaDescribeTopicService(parser)
			service.HandleRequest(domain.Request{Data: tt.data})
		})
	}
}
