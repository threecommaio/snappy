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
  snappy [command]

Available Commands:
  backup      Creates a snapshot and uploads to an S3 bucket
  help        Help about any command
  restore     Restores a snapshot from S3
  version

Flags:
      --debug   enable debug logging
  -h, --help    help for snappy

Use "snappy [command] --help" for more information about a command.
```