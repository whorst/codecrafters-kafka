package cluster_metadata_repository

import "testing"

func TestClusterMetadata_ParseClusterMetadataFileByTopicNames(t *testing.T) {
	parser := NewClusterMetadataRepository()
	parser.GetClusterMetadata()
}
