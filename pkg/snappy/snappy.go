package snappy

import (
	"log"
	"os"

	humanize "github.com/dustin/go-humanize"
)

// Backup a nodes snapshot to S3
func Backup(config *AWSConfig, snapshotID string) {
	s3 := NewS3(config)
	cassandra := NewCassandra()

	_, err := cassandra.CreateSnapshot(snapshotID)
	if err != nil {
		log.Println("snapshot already exists, going to continue upload anyway")
	}

	var totalSize int64

	files := cassandra.GetSnapshotFiles(snapshotID)
	for path, key := range files {
		log.Println("Uploading file:", key)
		s3.UploadFile(path, key)

		fi, e := os.Stat(path)
		if e != nil {
			log.Fatal(e)
		}
		totalSize += fi.Size()
	}
	log.Println("Uploaded a total size of:", humanize.Bytes(uint64(totalSize)))
}
