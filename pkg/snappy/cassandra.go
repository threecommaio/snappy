package snappy

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var searchPaths = []string{
	"/etc/cassandra",
	"/etc/cassandra/conf",
	"/etc/dse/cassandra",
	"/etc/dse",
	"/usr/local/share/cassandra",
	"/usr/local/share/cassandra/conf",
	"/opt/cassandra",
	"/opt/cassandra/conf",
	"/usr/bin",
	"/usr/sbin",
	"/usr/local/etc/cassandra",
	"/usr/local/etc/cassandra/conf",
	"/usr/local/bin",
	"/usr/local/sbin",
}

var errSnapshotExists = errors.New("snapshot already exists")
var errNodetoolError = errors.New("exit status 1 - nodetool connection error (is cassandra running?)")

type Cassandra struct {
	config   map[string]interface{}
	filename string
}

func find(filename string) string {
	for _, p := range searchPaths {
		var pathFilename = filepath.Join(p, filename)
		if _, err := os.Stat(pathFilename); err == nil {
			return pathFilename
		}
	}
	log.Fatalln(filename, "not found")
	return ""
}

func NewCassandra() *Cassandra {
	configFilename := cassandraYaml()

	config, err := parseYamlFile(configFilename)
	if err != nil {
		log.Fatal(err)
	}

	return &Cassandra{config: config, filename: configFilename}
}

func (c *Cassandra) GetConfigFilename() string {
	return c.filename
}

func nodeTool() string {
	return find("nodetool")
}

func cassandraYaml() string {
	return find("cassandra.yaml")
}

// CreateSnapshotID generates a new monotic snapshot id
func (c *Cassandra) CreateSnapshotID() string {
	return time.Now().Format("2006-01-02_150405")
}

// CreateSnapshot creates a snapshot by ID
func (c *Cassandra) CreateSnapshot(id string) (bool, error) {
	nodeTool := nodeTool()
	log.Infof("creating a snapshot using id [%s]\n", id)
	cmd := exec.Command(nodeTool, "snapshot", "-t", id)

	if err := cmd.Start(); err != nil {
		return false, err
	}

	log.Debug("waiting for nodetool to complete")
	if err := cmd.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				if status.ExitStatus() == 2 {
					return false, errors.Wrap(errSnapshotExists, id)
				}

				return false, errNodetoolError
			}
		} else {
			return false, errors.Errorf("cmd.Wait: %v", err)
		}
	}
	return true, nil
}

// GetDataDirectories returns a list of data directories defined in the config
func (c *Cassandra) GetDataDirectories() []string {
	var directories []string
	if dataDirs, ok := c.config["data_file_directories"]; ok {
		for _, dir := range dataDirs.([]interface{}) {
			directories = append(directories, dir.(string))
		}
	}
	return directories
}

func (c *Cassandra) GetSnapshotFiles(id string) map[string]string {

	var (
		keyspaces []string
		s3Files   = make(map[string]string)
	)
	dataDirs := c.GetDataDirectories()
	node := c.GetListenAddress()

	log.Debugln("fetched dataDirs", dataDirs)
	log.Debugln("fetched node ip address", node)

	for _, dataDir := range dataDirs {
		files, err := ioutil.ReadDir(dataDir)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range files {
			if file.IsDir() {
				keyspaces = append(keyspaces, file.Name())
			}
		}

		for _, keyspace := range keyspaces {

			files, err := ioutil.ReadDir(filepath.Join(dataDir, keyspace))
			if err != nil {
				log.Fatal(err)
			}

			var tables []string

			for _, file := range files {
				if file.IsDir() {
					tables = append(tables, file.Name())
				}
			}

			for _, table := range tables {
				// check if keyspace, table, snapshot exist
				tableDir := filepath.Join(dataDir, keyspace, table, "snapshots", id, "/")
				if _, err := os.Stat(tableDir); os.IsNotExist(err) {
					continue
				}

				filepath.Walk(tableDir, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						log.Fatal(err)
					}
					if !info.IsDir() {
						remotePath := strings.TrimPrefix(path, tableDir)
						s3Files[path] = filepath.Join("backups", id, node, keyspace, table, remotePath)
					}
					return nil
				})
			}
		}
	}

	return s3Files
}

// GetListenAddress returns the listen_address from the config
func (c *Cassandra) GetListenAddress() string {
	if val, ok := c.config["listen_address"]; ok {
		return val.(string)
	}

	localIP, err := GetLocalIP()
	if err != nil {
		log.Fatal(err)
	}
	log.Warnf("could not find a listen_address in cassandra.yaml, falling back to using %s\n", localIP)
	return localIP
}

// GetTokenRange finds the range of tokens for an ip address in cluster
func (c *Cassandra) GetTokenRange(ip string) ([]string, error) {
	nodeTool := exec.Command(nodeTool(), "ring")
	grep := exec.Command("grep", "-w", ip)
	awk := exec.Command("awk", "{print $NF \",\"}")
	xargs := exec.Command("xargs")

	output, _, err := Pipeline(nodeTool, grep, awk, xargs)
	if err != nil {
		return nil, err
	}
	ranges := string(output)

	ranges = strings.TrimSpace(ranges)
	ranges = strings.Replace(ranges, " ", "", -1)
	ranges = strings.Replace(ranges, "\u0000", "", -1)
	ranges = strings.TrimSuffix(ranges, ",")
	return strings.Split(ranges, ","), nil
}

func (c *Cassandra) FindTablePath(keyspace string, table string) (string, error) {
	dataDirs := c.GetDataDirectories()
	for _, dataDir := range dataDirs {
		keyspaceDir := filepath.Join(dataDir, keyspace)
		if _, err := os.Stat(keyspaceDir); os.IsNotExist(err) {
			continue
		}

		files, err := ioutil.ReadDir(keyspaceDir)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range files {
			if file.IsDir() {
				tableName, _ := Split(file.Name(), "-")
				if tableName == table {
					return filepath.Join(keyspaceDir, file.Name()), nil
				}
			}
		}
	}
	return "", errors.New("could not find table")
}

func (c *Cassandra) FindTableUUID(keyspace string, table string) (string, error) {
	dataDirs := c.GetDataDirectories()
	for _, dataDir := range dataDirs {
		keyspaceDir := filepath.Join(dataDir, keyspace)
		if _, err := os.Stat(keyspaceDir); os.IsNotExist(err) {
			continue
		}

		files, err := ioutil.ReadDir(keyspaceDir)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range files {
			if file.IsDir() {
				tableName, uuid := Split(file.Name(), "-")
				if tableName == table {
					return uuid, nil
				}
			}
		}
	}
	return "", errors.New("could not find table uuid")
}
