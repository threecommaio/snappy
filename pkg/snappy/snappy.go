package snappy

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cheggaaa/pb"
	humanize "github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
)

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

// Backup a nodes snapshot to S3
func Backup(config *AWSConfig, snapshotID string) {
	var totalSize int64

	s3, err := NewS3(config)
	if err != nil {
		log.Fatal(err)
	}
	cassandra := NewCassandra()

	_, err = cassandra.CreateSnapshot(snapshotID)
	if err != nil {
		log.Warn("snapshot already exists, going to continue upload anyway")
	}

	files := cassandra.GetSnapshotFiles(snapshotID)

	for path := range files {
		fi, e := os.Stat(path)
		if e != nil {
			log.Fatal(e)
		}
		totalSize += fi.Size()
	}

	bar := pb.New64(totalSize)
	bar.SetUnits(pb.U_BYTES)
	bar.Start()
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
	log.Infoln("uploaded a total size of:", humanize.Bytes(uint64(totalSize)))
}

// Prepare a mapping file to be written
func RestorePrepare(config *PrepareConfig) []byte {
	cassandra := NewCassandra()
	mappingConfig := &PrepareMapping{
		ClusterName: config.ClusterName,
	}

	if len(config.SourceNodes) != len(config.DestinationNodes) {
		log.Fatal("the number of source nodes must match the number of destination nodes")
	}

	for idx, srcNode := range config.SourceNodes {
		dstNode := config.DestinationNodes[idx]
		tokenRange, err := cassandra.GetTokenRange(srcNode)
		if err != nil {
			log.Fatal(err)
		}
		nodeMapping := &NodeMapping{Source: srcNode, Destination: dstNode, TokenRange: tokenRange}
		mappingConfig.Nodes = append(mappingConfig.Nodes, *nodeMapping)
	}

	b, err := json.MarshalIndent(mappingConfig, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func RestoreApply(dstNode string, mapping *PrepareMapping) {
	var tokenRange []string
	var initalToken string

	cassandra := NewCassandra()

	if _, ok := cassandra.config["initial_token"]; ok {
		log.Fatal("initial_token has already been set.. aborting")
	}

	for _, node := range mapping.Nodes {
		if dstNode == node.Destination {
			tokenRange = node.TokenRange
			break
		}
	}

	if tokenRange != nil {
		f, err := os.OpenFile(cassandra.GetConfigFilename(), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatal(err)
		}

		defer f.Close()
		initalToken = fmt.Sprintf("initial_token: %s\n", strings.Join(tokenRange, ", "))
		if _, err = f.WriteString(initalToken); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatalf("could not find node: %s in mapping file", dstNode)
	}
}
