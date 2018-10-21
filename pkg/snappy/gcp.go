package snappy

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aybabtme/iocontrol"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// CloudStorage represents the storage system under Google Cloud Platform
type CloudStorage struct {
	config *CloudConfig
}

// NewCloudStorage returns a new cloud storage
func NewCloudStorage(config *CloudConfig) (*CloudStorage, error) {
	return &CloudStorage{config: config}, nil
}

// UploadFile handles taking files and uploading them to Cloud Storage
func (c *CloudStorage) UploadFile(ctx context.Context, filename string, key string) error {
	var reader io.Reader

	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	if c.config.Throttle == 0 {
		reader = f
	} else {
		maxBurst := 5 * time.Second
		readPerSec := c.config.Throttle * Mbps
		measured := iocontrol.NewMeasuredReader(f)
		reader = iocontrol.ThrottledReader(measured, readPerSec, maxBurst)
	}

	// upload file
	log.Debugf("uploading file [%s] -> [%s]", filename, key)
	wc := client.Bucket(c.config.Bucket).Object(key).NewWriter(ctx)
	if _, err = io.Copy(wc, reader); err != nil {
		return errors.Wrapf(err, "error uploading %s", filename)
	}

	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

// IsSnapshotComplete checks if a previous uploaded snapshot was completely uploaded
func (c *CloudStorage) IsSnapshotComplete(ctx context.Context, path string) bool {
	key := filepath.Join(path, SnapshotCompleted)
	client, err := storage.NewClient(ctx)
	if err != nil {
		return false
	}
	_, err = client.Bucket(c.config.Bucket).Object(key).Attrs(ctx)
	return err == nil
}

// MarkSnapshotComplete marks a snapshot as completely uploaded
func (c *CloudStorage) MarkSnapshotComplete(ctx context.Context, prefix, snapshotID string) bool {
	key := filepath.Join(prefix, snapshotID, SnapshotCompleted)

	client, err := storage.NewClient(ctx)
	if err != nil {
		return false
	}
	wc := client.Bucket(c.config.Bucket).Object(key).NewWriter(ctx)
	r := strings.NewReader("")
	if _, err = io.Copy(wc, r); err != nil {
		return false
	}
	if err := wc.Close(); err != nil {
		return false
	}
	return true
}
