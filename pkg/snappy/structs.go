package snappy

type PrepareConfig struct {
	ClusterName      string
	SourceNodes      []string
	DestinationNodes []string
}

type PrepareMapping struct {
	ClusterName string        `json:"cluster_name"`
	Nodes       []NodeMapping `json:"nodes"`
}
type NodeMapping struct {
	Source      string
	Destination string
	TokenRange  []string
}

type Snapshot struct {
	Keyspace string
	Tables   []SnapshotTable
}

type SnapshotTable struct {
	Name    string
	SrcUUID string
	DstUUID string
}
