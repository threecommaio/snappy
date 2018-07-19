package snappy

import (
	"log"
	"os"

	"github.com/cheggaaa/pb"
	humanize "github.com/dustin/go-humanize"
)

// Backup a nodes snapshot to S3
func Backup(config *AWSConfig, snapshotID string) {
	var (
		totalSize int64
	)

	s3, err := NewS3(config)
	if err != nil {
		log.Fatal(err)
	}
	cassandra := NewCassandra()

	_, err = cassandra.CreateSnapshot(snapshotID)
	if err != nil {
		log.Println("snapshot already exists, going to continue upload anyway")
	}

	files := cassandra.GetSnapshotFiles(snapshotID)

	for path := range files {
		fi, e := os.Stat(path)
		if e != nil {
			log.Fatal(e)
		}
		totalSize += fi.Size()
	}

	bar := pb.New64(totalSize).Start()
	bar.SetUnits(pb.U_BYTES)
	bar.ShowSpeed = true

	for path, key := range files {
		if err := s3.UploadFile(path, key); err != nil {
			log.Fatal(err)
		}
		fi, e := os.Stat(path)
		if e != nil {
			log.Fatal(e)
		}
		bar.Add64(fi.Size())
	}
	bar.Finish()
	log.Println("Uploaded a total size of:", humanize.Bytes(uint64(totalSize)))
}
