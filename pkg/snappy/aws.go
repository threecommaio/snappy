package snappy

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/aybabtme/iocontrol"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type S3 struct {
	bucket     string
	throttle   int
	svc        *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

func NewS3(config *CloudConfig) (*S3, error) {
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		log.Fatalln("unable to load SDK config,", err.Error())
	}
	cfg.Region = config.Region

	svc := s3.New(cfg)
	req := svc.HeadBucketRequest(&s3.HeadBucketInput{Bucket: &config.Bucket})
	_, err = req.Send()
	if err != nil {
		log.Info(err)
		return nil, errors.Errorf("bucket [%s] not found or you do not have sufficient permissions", config.Bucket)
	}

	return &S3{
		bucket:     config.Bucket,
		throttle:   config.Throttle,
		svc:        svc,
		uploader:   s3manager.NewUploader(cfg),
		downloader: s3manager.NewDownloader(cfg),
	}, nil
}

// UploadFile handles taking files and uploading them to S3
func (s *S3) UploadFile(ctx context.Context, filename string, key string) error {
	var reader io.Reader

	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	if s.throttle == 0 {
		reader = f
	} else {
		maxBurst := 5 * time.Second
		readPerSec := s.throttle * Mbps
		measured := iocontrol.NewMeasuredReader(f)
		reader = iocontrol.ThrottledReader(measured, readPerSec, maxBurst)
	}

	// details of file to upload
	params := &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Body:   reader,
		Key:    aws.String(key),
	}

	// upload file
	log.Debugf("uploading file [%s] -> [%s]", filename, key)
	_, err = s.uploader.Upload(params, func(u *s3manager.Uploader) {
		u.MaxUploadParts = 10000       // set to maximum allowed by s3
		u.PartSize = 128 * 1024 * 1024 // 128MB
	})
	if err != nil {
		return errors.Wrapf(err, "error uploading %s", filename)
	}
	return nil
}

// DownloadFiles handles downloading concurrently multiple files from the bucket as quickly as possible
// This method will check if existing files were already downloaded and skip those if necessary
func (s *S3) DownloadFiles(snapshotPath string, keys []string, directory string) error {
	var wg sync.WaitGroup
	for _, key := range keys {
		filePath := strings.TrimPrefix(key, snapshotPath)
		splitPath := strings.Split(filePath, "/")
		trimPath := strings.Join(splitPath[2:], "/")
		dirFolder := filepath.Dir(filepath.Join(directory, trimPath))

		if _, err := os.Stat(dirFolder); err != nil {
			log.Debugf("Creating directory: %s", dirFolder)
			if err := os.MkdirAll(dirFolder, 0755); err != nil {
				log.Fatal(err)
			}
		}

		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			localFile := filepath.Join(directory, trimPath)

			// check if this file already exists, to avoid re-downloading
			if f, err := os.Stat(localFile); err == nil {
				// file was found lets compare snapshot size with local size to make sure it was fully downloaded
				params := &s3.HeadObjectInput{
					Bucket: aws.String(s.bucket),
					Key:    aws.String(key),
				}

				req := s.svc.HeadObjectRequest(params)
				result, err := req.Send()

				if err == nil && *result.ContentLength == f.Size() {
					log.Debugf("file was already downloaded, skipping: %s", localFile)
					return
				}
			}

			diskFile, err := os.Create(localFile)
			defer diskFile.Close()

			if err != nil {
				log.Fatal(err)
			}

			params := &s3.GetObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    aws.String(key),
			}

			_, err = s.downloader.Download(diskFile, params)
			if err != nil {
				log.Fatal(err)
			}
			log.Debugf("Downloaded file: %s", filepath.Join(directory, trimPath))
		}(key)
	}
	wg.Wait()

	return nil
}

// IsSnapshotComplete checks if a previous uploaded snapshot was completely uploaded
func (s *S3) IsSnapshotComplete(ctx context.Context, path string) bool {
	key := filepath.Join(path, SnapshotCompleted)
	req := s.svc.HeadObjectRequest(&s3.HeadObjectInput{Bucket: &s.bucket, Key: aws.String(key)})
	_, err := req.Send()

	return err == nil
}

// MarkSnapshotComplete marks a snapshot as completely uploaded
func (s *S3) MarkSnapshotComplete(ctx context.Context, prefix, snapshotID string) bool {
	key := filepath.Join(prefix, snapshotID, SnapshotCompleted)

	params := &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Body:   strings.NewReader(""),
		Key:    aws.String(key),
	}

	// upload file
	_, err := s.uploader.Upload(params)

	return err == nil
}

// ListKeyspaces returns a set of keyspaces found on the bucket
func (s *S3) ListKeyspaces(path string) []string {
	var keyspaces []string

	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
	}
	req := s.svc.ListObjectsV2Request(params)

	p := req.Paginate()
	for p.Next() {
		page := p.CurrentPage()
		for _, obj := range page.CommonPrefixes {
			keyspace := strings.TrimSuffix(strings.TrimPrefix(*obj.Prefix, *page.Prefix), "/")
			// do not include system_* keyspaces
			if !strings.HasPrefix(keyspace, "system") {
				keyspaces = append(keyspaces, keyspace)
			}
		}
	}

	if err := p.Err(); err != nil {
		log.Fatalf("failed to list objects, %v", err)
	}

	return keyspaces
}

// ListTables returns a set of tables found on the bucket from a keyspace
func (s *S3) ListTables(path string, keyspace string) []string {
	var tables []string

	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(filepath.Join(path, keyspace) + "/"),
		Delimiter: aws.String("/"),
	}
	req := s.svc.ListObjectsV2Request(params)
	p := req.Paginate()
	for p.Next() {
		page := p.CurrentPage()
		for _, obj := range page.CommonPrefixes {
			table := strings.TrimSuffix(strings.TrimPrefix(*obj.Prefix, *page.Prefix), "/")
			tables = append(tables, table)
		}
	}

	if err := p.Err(); err != nil {
		log.Fatalf("failed to list objects, %v", err)
	}

	return tables
}

func (s *S3) ListSnapshotFiles(path string, keyspace string, table string, uuid string) []string {
	var files []string
	var tableName = table + "-" + uuid

	relPath := filepath.Join(path, keyspace, tableName)

	log.Debugf("s3 path: %s", relPath)

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(relPath),
	}
	req := s.svc.ListObjectsV2Request(params)
	p := req.Paginate()
	for p.Next() {
		page := p.CurrentPage()
		for _, obj := range page.Contents {
			files = append(files, *obj.Key)
		}
	}

	if err := p.Err(); err != nil {
		log.Fatalf("failed to list objects, %v", err)
	}

	return files
}
