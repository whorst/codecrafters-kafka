package domain

type PartitionToFetch struct {
	TopicName          string
	PartitionIndex     int
	TopicFetchResponse *FetchResponsePartition
}

type MessageFetchRequest struct {
	PartitionsToFetch []PartitionToFetch
}
