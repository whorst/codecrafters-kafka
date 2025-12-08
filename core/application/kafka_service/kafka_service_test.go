package kafka_service

import (
	"encoding/binary"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/core/domain"
	"github.com/codecrafters-io/kafka-starter-go/core/ports/parser"
)

// mockParser is a mock implementation of ProtocolParser for testing
type mockParser struct {
	parseRequestFunc   func(data []byte) (*parser.ParsedRequest, error)
	encodeResponseFunc func(response *parser.ResponseData) ([]byte, error)
}

func (m *mockParser) ParseRequest(data []byte) (*parser.ParsedRequest, error) {
	if m.parseRequestFunc != nil {
		return m.parseRequestFunc(data)
	}
	return nil, nil
}

func (m *mockParser) EncodeResponse(response *parser.ResponseData) ([]byte, error) {
	if m.encodeResponseFunc != nil {
		return m.encodeResponseFunc(response)
	}
	// Default: just return the error code bytes for testing
	return response.ErrorCode, nil
}

func TestKafkaService_HandleRequest_ErrorCodeForValidAPIVersion(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion int
		wantErr    bool
	}{
		{"API version 0", 0, false},
		{"API version 1", 1, false},
		{"API version 2", 2, false},
		{"API version 3", 3, false},
		{"API version 4", 4, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock parser that returns the specified API version
			mockParser := &mockParser{
				parseRequestFunc: func(data []byte) (*parser.ParsedRequest, error) {
					return &parser.ParsedRequest{
						CorrelationID: []byte{0x00, 0x00, 0x00, 0x01},
						APIVersion:    tt.apiVersion,
					}, nil
				},
				encodeResponseFunc: func(response *parser.ResponseData) ([]byte, error) {
					// Return just the error code for easy verification
					return response.ErrorCode, nil
				},
			}

			service := NewKafkaService(mockParser)
			req := domain.Request{Data: []byte{0x00, 0x00, 0x00, 0x00}}

			resp, err := service.HandleRequest(req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HandleRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// For valid API versions, error code should be 0 (0x00, 0x00)
			if len(resp.Data) < 2 {
				t.Errorf("HandleRequest() response too short, got %d bytes", len(resp.Data))
				return
			}

			errorCode := binary.BigEndian.Uint16(resp.Data[:2])
			if errorCode != 0 {
				t.Errorf("HandleRequest() error code = %d, want 0 for API version %d", errorCode, tt.apiVersion)
			}
		})
	}
}

func TestKafkaService_HandleRequest_ErrorCodeForInvalidAPIVersion(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion int
		wantErr    bool
	}{
		{"API version -1", -1, false},
		{"API version 5", 5, false},
		{"API version 10", 10, false},
		{"API version 100", 100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock parser that returns the specified API version
			mockParser := &mockParser{
				parseRequestFunc: func(data []byte) (*parser.ParsedRequest, error) {
					return &parser.ParsedRequest{
						CorrelationID: []byte{0x00, 0x00, 0x00, 0x01},
						APIVersion:    tt.apiVersion,
					}, nil
				},
				encodeResponseFunc: func(response *parser.ResponseData) ([]byte, error) {
					// Return just the error code for easy verification
					return response.ErrorCode, nil
				},
			}

			service := NewKafkaService(mockParser)
			req := domain.Request{Data: []byte{0x00, 0x00, 0x00, 0x00}}

			resp, err := service.HandleRequest(req)
			if (err != nil) != tt.wantErr {
				t.Errorf("HandleRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// For invalid API versions, error code should be 35 (0x00, 0x23)
			if len(resp.Data) < 2 {
				t.Errorf("HandleRequest() response too short, got %d bytes", len(resp.Data))
				return
			}

			errorCode := binary.BigEndian.Uint16(resp.Data[:2])
			if errorCode != 35 {
				t.Errorf("HandleRequest() error code = %d, want 35 for invalid API version %d", errorCode, tt.apiVersion)
			}
		})
	}
}

func TestKafkaService_determineErrorCode(t *testing.T) {
	service := &KafkaService{}

	tests := []struct {
		name       string
		apiVersion int
		want       uint16
	}{
		{"Invalid: API version -1", -1, 35},
		{"Invalid: API version 5", 5, 35},
		{"Invalid: API version 10", 10, 35},
		{"Invalid: API version 100", 100, 35},
		{"Valid: API version 0", 0, 0},
		{"Valid: API version 1", 1, 0},
		{"Valid: API version 2", 2, 0},
		{"Valid: API version 3", 3, 0},
		{"Valid: API version 4", 4, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := service.determineErrorCode(tt.apiVersion)
			errorCode := binary.BigEndian.Uint16(got)
			if errorCode != tt.want {
				t.Errorf("determineErrorCode(%d) = %d, want %d", tt.apiVersion, errorCode, tt.want)
			}
		})
	}
}
