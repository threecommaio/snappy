// Copyright © 2018 ThreeComma.io <hello@threecomma.io>

package cmd

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/threecommaio/snappy/pkg/snappy"
)

func init() {
	restoreCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().Bool("skip-tables", false, "skip tables that might be missing from schema")
	downloadCmd.Flags().StringP("node", "n", "", "the ip address of the destination node")
	downloadCmd.Flags().StringP("snapshot-id", "s", "", "snapshot id")
	downloadCmd.Flags().StringP("aws-region", "r", "", "the aws region to use")
	downloadCmd.Flags().StringP("aws-s3-bucket", "b", "", "the aws s3 bucket to use")

	downloadCmd.MarkFlagRequired("node")
	downloadCmd.MarkFlagRequired("snapshot-id")
	downloadCmd.MarkFlagRequired("aws-region")
	downloadCmd.MarkFlagRequired("aws-s3-bucket")

}

// downloadCmd represents the download command
var downloadCmd = &cobra.Command{
	Use:   "download [mapping-file.json]",
	Short: "Download data from a snappy snapshot",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var (
			node, _       = cmd.Flags().GetString("node")
			region, _     = cmd.Flags().GetString("aws-region")
			bucket, _     = cmd.Flags().GetString("aws-s3-bucket")
			snapshotID, _ = cmd.Flags().GetString("snapshot-id")
			skipTables, _ = cmd.Flags().GetBool("skip-tables")
			config        = &snappy.AWSConfig{Bucket: bucket, Region: region}
			fs            = afero.NewOsFs()
		)
		if mappingFile, err := afero.ReadFile(fs, args[0]); err == nil {
			prepareMapping := &snappy.PrepareMapping{}
			if err := json.Unmarshal(mappingFile, &prepareMapping); err != nil {
				log.Fatal(err)
			}
			snappy.DownloadSnapshot(fs, node, snapshotID, config, prepareMapping, skipTables)
		} else {
			log.Fatal(err)
		}
	},
}
