package fetch

type FetchParser interface {
	// ParseRequest extracts structured data from raw binary request data
	ParseRequest(data []byte) (*ParsedRequestFetch, error)

	// EncodeResponse converts a response into binary format
	EncodeResponse(response *ResponseDataFetch) ([]byte, error)
}

// ParsedRequestFetch represents a parsed Kafka Fetch request
type ParsedRequestFetch struct {
	// Header fields
	APIKey        int    // API Key (1 for Fetch)
	APIVersion    int    // API Version (16 in the example)
	CorrelationID []byte // Correlation ID (4 bytes)
	ClientID      string // Client ID string

	// Body fields
	MaxWaitMS      int32  // Maximum wait time in milliseconds (4 bytes INT32)
	MinBytes       int32  // Minimum bytes to fetch (4 bytes INT32)
	MaxBytes       int32  // Maximum bytes to fetch (4 bytes INT32)
	IsolationLevel int8   // Isolation level (1 byte)
	SessionID      int32  // Session ID (4 bytes INT32)
	SessionEpoch   int32  // Session epoch (4 bytes INT32)
	Topics         []FetchTopic
	ForgottenTopics []ForgottenTopic
	RackID         string // Rack ID string
}

// FetchTopic represents a topic in the Fetch request
type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

// FetchPartition represents a partition in the Fetch request
type FetchPartition struct {
	PartitionIndex int32
	CurrentLeaderEpoch int32
	FetchOffset     int64
	LastFetchedEpoch int32
	LogStartOffset  int64
	PartitionMaxBytes int32
}

// ForgottenTopic represents a forgotten topic in the Fetch request
type ForgottenTopic struct {
	Name       string
	Partitions []int32
}

type ApiKey struct {
	ApiKey         []byte // Hard Coded to 2 byte
	MinVersion     []byte // Hard coded to 2 bytes
	MaxVersion     []byte // Hard Coded to 2 bytes
	TagBufferChild []byte // Hard Coded to 1 byte
}

// ResponseDataFetch represents the data needed to build a response
type ResponseDataFetch struct {
	CorrelationID      []byte // Correlation ID (Unknown)
	ErrorCode          []byte // Error code (2 bytes)
	ApiKeysArrayLength []byte // Hard Coded to 1 byte
	// ================ Everything below will eventually be a part of an array ================
	ApiKeys []ApiKey
	// ================ End Array ==========================
	ThrottleTimeMs  []byte // Hard Coded to 4 bytes
	TagBufferParent []byte // Hard Coded to 1 byte
	SessionId       []byte // Hard Coded to 4 bytes

}
