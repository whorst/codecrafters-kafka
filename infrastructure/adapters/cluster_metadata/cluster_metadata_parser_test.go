package cluster_metadata

import "testing"

func TestClusterMetadata_ParseClusterMetadataFileByTopicNames(t *testing.T) {
	parser := NewClusterMetadataParser()
	parser.ParseClusterMetadataFileByTopicNames()
}
