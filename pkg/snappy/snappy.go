package snappy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	humanize "github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
)

const (
	SnapshotFolderPrefix = "backups"
)

// Backup a nodes snapshot to a cloud provider
func Backup(config *CloudConfig, snapshotID string, keyspaces []string) error {
	var totalSize int64

	cassandra := NewCassandra()
	_, err := cassandra.CreateSnapshot(snapshotID, keyspaces)
	if err != nil {
		log.Warn("snapshot already exists, going to continue upload anyway")
	}

	nodeIP := cassandra.GetListenAddress()
	dataDirs := cassandra.GetDataDirectories()
	files, err := cassandra.GetSnapshotFiles(snapshotID, nodeIP, "backups", dataDirs)
	if err != nil {
		return err
	}

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

	var cu CloudSnapshot

	switch config.Provider {
	case CloudProviderAWS:
		cu, err = NewS3(config)
		if err != nil {
			log.Fatal(err)
		}
	case CloudProviderGCP:
		cu, err = NewCloudStorage(config)
		if err != nil {
			log.Fatal(err)
		}
	}

	for path, key := range files {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		if err := cu.UploadFile(ctx, path, key); err != nil {
			return err
		}
		fi, e := os.Stat(path)
		if e != nil {
			return err
		}
		bar.Add64(fi.Size())
	}
	bar.Finish()
	log.Infoln("uploaded a total size of:", humanize.Bytes(uint64(totalSize)))
	return nil
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

// RestoreApply handles the configuration of cassandra.yaml to make the destination node match the old source node
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
		autoBootstrap := fmt.Sprintf("auto_bootstrap: false\n")
		if _, err = f.WriteString(autoBootstrap); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatalf("could not find node: %s in mapping file", dstNode)
	}
}

// DownloadSnapshot handles copying data from a snapshot on cloud provider to the local node
func DownloadSnapshot(dstNode string, snapshotID string, config *CloudConfig, mapping *PrepareMapping, skipTables bool) {
	var (
		srcNode       string
		snapshotIndex []Snapshot
		nodeFound     = false
		cassandra     = NewCassandra()
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

	s3, err := NewS3(config)
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
