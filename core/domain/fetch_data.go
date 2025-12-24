package domain

type ParsedRequestFetch struct {
	// Header fields
	APIKey        int    // API Key (1 for Fetch)
	APIVersion    int    // API Version (16 in the example)
	CorrelationID []byte // Correlation ID (4 bytes)
	ClientID      string // Client ID string

	// Body fields
	MaxWaitMS       int32 // Maximum wait time in milliseconds (4 bytes INT32)
	MinBytes        int32 // Minimum bytes to fetch (4 bytes INT32)
	MaxBytes        int32 // Maximum bytes to fetch (4 bytes INT32)
	IsolationLevel  int8  // Isolation level (1 byte)
	SessionID       int32 // Session ID (4 bytes INT32)
	SessionEpoch    int32 // Session epoch (4 bytes INT32)
	Topics          []FetchTopic
	ForgottenTopics []ForgottenTopic
	RackID          string // Rack ID string
}

// FetchTopic represents a topic in the Fetch request
type FetchTopic struct {
	Name           string
	Partitions     []FetchPartition
	TopicNameBytes []byte
}

// FetchPartition represents a partition in the Fetch request
type FetchPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

// ForgottenTopic represents a forgotten topic in the Fetch request
type ForgottenTopic struct {
	Name       string
	Partitions []int32
}

// ResponseDataFetch represents the data needed to build a Fetch response
type ResponseDataFetch struct {
	CorrelationID  []byte // Correlation ID (4 bytes)
	ThrottleTimeMs int32  // Throttle time in milliseconds (4 bytes INT32)
	ErrorCode      int16  // Error code (2 bytes INT16)
	SessionID      int32  // Session ID (4 bytes INT32)
	Topics         []FetchResponseTopic
}

// FetchResponseTopic represents a topic in the Fetch response
type FetchResponseTopic struct {
	TopicName      string
	Partitions     []FetchResponsePartition
	TopicNameBytes []byte
}

// FetchResponsePartition represents a partition in the Fetch response
type FetchResponsePartition struct {
	PartitionIndex       int32                // Partition index (4 bytes INT32)
	ErrorCode            int16                // Error code (2 bytes INT16)
	HighWatermark        int64                // High watermark offset (8 bytes INT64)
	LastStableOffset     int64                // Last stable offset (8 bytes INT64)
	LogStartOffset       int64                // Log start offset (8 bytes INT64)
	AbortedTransactions  []AbortedTransaction // Aborted transactions array
	PreferredReadReplica int32                // Preferred read replica (4 bytes INT32)
	Records              []byte               // Record set (variable length)
}

// AbortedTransaction represents an aborted transaction
type AbortedTransaction struct {
	ProducerID  int64 // Producer ID (8 bytes INT64)
	FirstOffset int64 // First offset (8 bytes INT64)
}
