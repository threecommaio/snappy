package snappy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cheggaaa/pb"
	humanize "github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	SnapshotFolderPrefix = "backups"
)

// Backup a nodes snapshot to S3
func Backup(fs afero.Fs, config *AWSConfig, snapshotID string) {
	var totalSize int64

	s3, err := NewS3(fs, config)
	if err != nil {
		log.Fatal(err)
	}
	cassandra := NewCassandra(fs)

	_, err = cassandra.CreateSnapshot(snapshotID)
	if err != nil {
		switch err {
		case errSnapshotExists:
			log.Warn("snapshot already exists, going to continue upload anyway")
		case errNodetoolError:
			log.Fatal(errNodetoolError)
		default:
			log.Fatal(err)
		}
	}

	files := cassandra.GetSnapshotFiles(snapshotID)

	for path := range files {
		fi, e := fs.Stat(path)
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
		fi, e := fs.Stat(path)
		if e != nil {
			log.Fatal(e)
		}
		bar.Add64(fi.Size())
	}
	bar.Finish()
	log.Infoln("uploaded a total size of:", humanize.Bytes(uint64(totalSize)))
}

// Prepare a mapping file to be written
func RestorePrepare(fs afero.Fs, config *PrepareConfig) []byte {
	cassandra := NewCassandra(fs)
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

// RestoreApply handles the configuration of cassandra.yaml to make the destination node match the old source node
func RestoreApply(fs afero.Fs, dstNode string, mapping *PrepareMapping) {
	var tokenRange []string
	var initalToken string

	cassandra := NewCassandra(fs)

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
		f, err := fs.OpenFile(cassandra.GetConfigFilename(), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatal(err)
		}

		defer f.Close()
		initalToken = fmt.Sprintf("initial_token: %s\n", strings.Join(tokenRange, ", "))
		if _, err = f.WriteString(initalToken); err != nil {
			log.Fatal(err)
		}
		autoBootstrap := fmt.Sprintf("auto_bootstrap: false\n")
		if _, err = f.WriteString(autoBootstrap); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatalf("could not find node: %s in mapping file", dstNode)
	}
}

// DownloadSnapshot handles copying data from a snapshot on S3 to the local node
func DownloadSnapshot(fs afero.Fs, dstNode string, snapshotID string, config *AWSConfig, mapping *PrepareMapping, skipTables bool) {
	var (
		srcNode       string
		snapshotIndex []Snapshot
		nodeFound     = false
		cassandra     = NewCassandra(fs)
	)

	for _, node := range mapping.Nodes {
		if dstNode == node.Destination {
			srcNode = node.Source
			nodeFound = true
			break
		}
	}

	if !nodeFound {
		log.Fatalf("could not find node: %s in mapping file", dstNode)
	}

	s3, err := NewS3(fs, config)
	if err != nil {
		log.Fatal(err)
	}

	snapshotFolder := filepath.Join(SnapshotFolderPrefix, snapshotID, srcNode) + "/"

	// find keyspaces associated to this snapshot
	keyspaces := s3.ListKeyspaces(snapshotFolder)
	for _, keyspace := range keyspaces {
		var snapshotTables []SnapshotTable

		// populate tables for each keyspace
		tables := s3.ListTables(snapshotFolder, keyspace)
		for _, table := range tables {
			tableName, srcUUID := Split(table, "-")
			dstUUID, err := cassandra.FindTableUUID(keyspace, tableName)

			if err != nil && !skipTables {
				log.Fatalf("tried to locate [keyspace: %s] [table: %s] on local filesystem, but it looks to be missing. check schema to make sure table exists. aborting...", keyspace, tableName)
			} else if err != nil && skipTables {
				log.Warnf("tried to locate [keyspace: %s] [table: %s] on local filesystem, but it looks to be missing. check schema to make sure table exists. skipping...", keyspace, tableName)
			} else {
				snapshotTable := &SnapshotTable{
					Name:    tableName,
					SrcUUID: srcUUID,
					DstUUID: dstUUID,
				}
				snapshotTables = append(snapshotTables, *snapshotTable)
			}
		}
		snapshot := &Snapshot{Keyspace: keyspace, Tables: snapshotTables}
		snapshotIndex = append(snapshotIndex, *snapshot)
	}

	// copy data from bucket to filesystem
	for _, index := range snapshotIndex {
		for _, table := range index.Tables {
			log.Infof("Downloading data to %s/%s", index.Keyspace, table.Name)
			remoteFiles := s3.ListSnapshotFiles(snapshotFolder, index.Keyspace, table.Name, table.SrcUUID)
			downloadFolder, err := cassandra.FindTablePath(index.Keyspace, table.Name)
			if err != nil {
				log.Fatal(err)
			}
			s3.DownloadFiles(snapshotFolder, remoteFiles, downloadFolder)
		}
	}

}
