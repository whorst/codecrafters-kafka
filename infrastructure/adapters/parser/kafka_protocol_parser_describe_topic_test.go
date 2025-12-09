package parser

import (
	"reflect"
	"testing"
)

var defaultData = []byte{
	0x00, 0x00, 0x00, 0x20, // Message size
	0x00, 0x4b, // API Key
	0x00, 0x00, // API Version
	0x00, 0x00, 0x00, 0x07, // Correlation ID
	0x00, 0x09, // Length
	0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // Contents
	0x00,             // Tag Buffer
	0x02,             // Array Length
	0x04,             // Topic Name Length
	0x66, 0x6f, 0x6f, // Topic Name
	0x00,                   // Topic Tag Buffer
	0x00, 0x00, 0x00, 0x64, // Response Partition Limit
	0xff, // Cursor
	0x00, // Tag Buffer
}

func TestKafkaProtocolParserDescribeTopic_ParseRequest(t *testing.T) {
	parser := NewKafkaProtocolParserDescribeTopic()

	result, err := parser.ParseRequest(defaultData)
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}

	if result == nil {
		t.Fatal("ParseRequest() returned nil result")
	}

	// Expected structure: &{[0 0 0 7] [{foo [0] [102 111 111]} {foo [0] [102 111 111]}] [0]}
	expectedCorrelationIdBytes := []byte{0x00, 0x00, 0x00, 0x07}
	if !reflect.DeepEqual(result.CorrelationIdBytes, expectedCorrelationIdBytes) {
		t.Errorf("CorrelationIdBytes = %v, want %v", result.CorrelationIdBytes, expectedCorrelationIdBytes)
	}

	// Expected topics: [{foo [0] [102 111 111]} {foo [0] [102 111 111]}]
	expectedTopicsCount := 1
	if len(result.Topics) != expectedTopicsCount {
		t.Fatalf("Topics count = %d, want %d", len(result.Topics), expectedTopicsCount)
	}

	expectedTopicName := "foo"
	expectedTagBuffer := []byte{0x00}
	expectedTopicNameBytes := []byte{102, 111, 111} // "foo" in ASCII

	for i, topic := range result.Topics {
		if topic.TopicName != expectedTopicName {
			t.Errorf("Topics[%d].TopicName = %q, want %q", i, topic.TopicName, expectedTopicName)
		}
		if !reflect.DeepEqual(topic.TagBuffer, expectedTagBuffer) {
			t.Errorf("Topics[%d].TagBuffer = %v, want %v", i, topic.TagBuffer, expectedTagBuffer)
		}
		if !reflect.DeepEqual(topic.TopicNameBytes, expectedTopicNameBytes) {
			t.Errorf("Topics[%d].TopicNameBytes = %v, want %v", i, topic.TopicNameBytes, expectedTopicNameBytes)
		}
	}

	// Expected TagBuffer: [0]
	expectedTagBufferFinal := []byte{0x00}
	if !reflect.DeepEqual(result.TagBuffer, expectedTagBufferFinal) {
		t.Errorf("TagBuffer = %v, want %v", result.TagBuffer, expectedTagBufferFinal)
	}
}
