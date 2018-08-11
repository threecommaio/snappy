package snappy

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/afero"
)

const cassandraYamlFilename = "/usr/local/etc/cassandra/cassandra.yaml"
const nodeToolFilename = "/usr/local/bin/nodetool"

func createFile(fs afero.Fs, filename string) {
	fs.MkdirAll(filepath.Dir(filename), 0644)
	f, _ := fs.Create(filename)
	defer f.Close()
}
func createCassandraYaml(fs afero.Fs) {
	createFile(fs, cassandraYamlFilename)
}

func createNodetool(fs afero.Fs) {
	createFile(fs, nodeToolFilename)
}

func Test_find(t *testing.T) {
	type args struct {
		fs       afero.Fs
		filename string
	}
	type test struct {
		name    string
		args    args
		want    string
		wantErr bool
	}
	fs := afero.NewMemMapFs()
	files := []string{cassandraYamlFilename, nodeToolFilename}
	tests := make([]test, len(files))

	for t, file := range files {
		fs.MkdirAll(filepath.Dir(file), 0644)
		f, _ := fs.Create(file)
		defer f.Close()

		tests[t] = test{file, args{fs: fs, filename: filepath.Base(file)}, file, false}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := find(tt.args.fs, tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("find() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("find() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewCassandra(t *testing.T) {
	fs := afero.NewMemMapFs()
	createCassandraYaml(fs)

	type args struct {
		fs afero.Fs
	}
	tests := []struct {
		name    string
		args    args
		want    *Cassandra
		wantErr bool
	}{
		{"cassandra", args{fs: fs}, &Cassandra{config: map[string]interface{}{},
			fs:       fs,
			filename: cassandraYamlFilename}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCassandra(tt.args.fs)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCassandra() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCassandra() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCassandra_GetConfigFilename(t *testing.T) {
	type fields struct {
		config   map[string]interface{}
		filename string
		fs       afero.Fs
	}
	fs := afero.NewMemMapFs()
	createCassandraYaml(fs)

	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"cassandra", fields{config: map[string]interface{}{},
			fs:       fs,
			filename: cassandraYamlFilename}, cassandraYamlFilename},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cassandra{
				config:   tt.fields.config,
				filename: tt.fields.filename,
				fs:       tt.fields.fs,
			}
			if got := c.GetConfigFilename(); got != tt.want {
				t.Errorf("Cassandra.GetConfigFilename() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nodeTool(t *testing.T) {
	type args struct {
		fs afero.Fs
	}
	fs := afero.NewMemMapFs()
	createNodetool(fs)

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"nodetool", args{fs: fs}, nodeToolFilename, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := nodeTool(tt.args.fs)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeTool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("nodeTool() = %v, want %v", got, tt.want)
			}
		})
	}
}
