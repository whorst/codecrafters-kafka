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

// ResponseDataFetch represents the data needed to build a Fetch response
type ResponseDataFetch struct {
	CorrelationID []byte // Correlation ID (4 bytes)
	ThrottleTimeMs int32 // Throttle time in milliseconds (4 bytes INT32)
	SessionID     int32  // Session ID (4 bytes INT32)
	Topics         []FetchResponseTopic
}

// FetchResponseTopic represents a topic in the Fetch response
type FetchResponseTopic struct {
	Name       string
	Partitions []FetchResponsePartition
}

// FetchResponsePartition represents a partition in the Fetch response
type FetchResponsePartition struct {
	PartitionIndex        int32  // Partition index (4 bytes INT32)
	ErrorCode             int16  // Error code (2 bytes INT16)
	HighWatermark         int64  // High watermark offset (8 bytes INT64)
	LastStableOffset      int64  // Last stable offset (8 bytes INT64)
	LogStartOffset        int64  // Log start offset (8 bytes INT64)
	AbortedTransactions   []AbortedTransaction // Aborted transactions array
	PreferredReadReplica  int32  // Preferred read replica (4 bytes INT32)
	Records               []byte // Record set (variable length)
}

// AbortedTransaction represents an aborted transaction
type AbortedTransaction struct {
	ProducerID  int64 // Producer ID (8 bytes INT64)
	FirstOffset int64 // First offset (8 bytes INT64)
}
