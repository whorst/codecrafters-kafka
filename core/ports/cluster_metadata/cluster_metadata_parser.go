package clutser_metadata

type ClusterMetadataParser interface {
	ParseClusterMetadataFileByTopicNames(topicNames []string) (error, ClusterMetadataLogResponse)
}

type ClusterMetadataLogResponse struct {
	TopicNameTopicMetadataInfoMap map[string]string
}

type TopicMetadataInfo struct {
	TopicUuids   map[string]string
	PartitionIds map[string]string
}
