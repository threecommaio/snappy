package snappy

import (
	"context"
	"math"
)

// CloudProvider represents the name of the provider
type CloudProvider string

// Cloud providers
const (
	CloudProviderAWS CloudProvider = "AWS"
	CloudProviderGCP CloudProvider = "GCP"
)

// CloudConfig handles the configuration for the cloud provider
type CloudConfig struct {
	Provider CloudProvider
	Region   string
	Bucket   string
	Throttle int
}

// CloudSnapshot is the interface for handling multiple cloud providers for uploading and download data in the cloud
type CloudSnapshot interface {
	UploadFile(ctx context.Context, filename string, key string) error
	IsSnapshotComplete(ctx context.Context, path string) bool
	MarkSnapshotComplete(ctx context.Context, prefix, snapshotID string) bool
}

// Basic consts
const (
	// Bytes per second
	BytesPerSecond int = 1
	// Kilobits per second
	Kbps = BytesPerSecond * (1024 / 8)
	// Megabits per second
	Mbps = Kbps * 1024
	// Gigabits per second
	Gbps = Mbps * 1024
	// Unlimited bandwidth
	Unlimited = math.MaxInt64

	SnapshotCompleted = "SNAPSHOT_COMPLETED"
)
