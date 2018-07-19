package snappy

import (
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/aybabtme/iocontrol"
	"github.com/pkg/errors"
)

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
)

type S3 struct {
	bucket   string
	throttle int
	svc      *s3.S3
	uploader *s3manager.Uploader
}

type AWSConfig struct {
	Region   string
	Bucket   string
	Throttle int
}

func NewS3(config *AWSConfig) (*S3, error) {
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}
	cfg.Region = config.Region

	svc := s3.New(cfg)

	req := svc.HeadBucketRequest(&s3.HeadBucketInput{Bucket: &config.Bucket})
	_, err = req.Send()
	if err != nil {
		return nil, errors.Errorf("bucket [%s] not found or you do not have sufficient permissions", config.Bucket)
	}

	return &S3{
		bucket:   config.Bucket,
		throttle: config.Throttle,
		svc:      svc,
		uploader: s3manager.NewUploader(cfg),
	}, nil
}

func (s *S3) UploadFile(filename string, key string) error {
	var reader io.Reader

	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	if s.throttle == 0 {
		reader = f
	} else {
		maxBurst := 100 * time.Millisecond
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
	_, err = s.uploader.Upload(params, func(u *s3manager.Uploader) {
		u.MaxUploadParts = 10000       // set to maximum allowed by s3
		u.PartSize = 128 * 1024 * 1024 // 128MB
	})
	if err != nil {
		return errors.Wrapf(err, "error uploading %s", filename)
	}
	return nil
}
