# Snappy
A tool to backup Cassandra snapshots to S3

## Installation

To build `snappy` run the following:
```
$ make build
$ ./snappy
```

## Usage
```
Cassandra backup and restore utility

Usage:
  snappy [flags]
  snappy [command]

Available Commands:
  backup      Creates a snapshot and uploads to an S3 bucket
  help        Help about any command

Flags:
  -h, --help   help for snappy

Use "snappy [command] --help" for more information about a command.
```