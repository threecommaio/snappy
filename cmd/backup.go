// Copyright © 2018 ThreeComma.io <hello@threecomma.io>

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/threecommaio/snappy/pkg/snappy"
)

// backupCmd represents the backup command
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Creates a snapshot and uploads to an S3 bucket",
	Run: func(cmd *cobra.Command, args []string) {
		var (
			region, _     = cmd.Flags().GetString("aws-region")
			bucket, _     = cmd.Flags().GetString("aws-s3-bucket")
			throttle, _   = cmd.Flags().GetInt("throttle")
			snapshotID, _ = cmd.Flags().GetString("snapshot-id")
			config        = &snappy.AWSConfig{Bucket: bucket, Region: region, Throttle: throttle}
		)
		snappy.Backup(config, snapshotID)
	},
}

func init() {
	rootCmd.AddCommand(backupCmd)

	backupCmd.Flags().StringP("snapshot-id", "s", "", "snapshot id")
	backupCmd.Flags().StringP("aws-region", "r", "", "the aws region to use")
	backupCmd.Flags().StringP("aws-s3-bucket", "b", "", "the aws s3 bucket to use")
	backupCmd.Flags().IntP("throttle", "t", 200, "throttle in megabits/s")

	backupCmd.MarkFlagRequired("snapshot-id")
	backupCmd.MarkFlagRequired("aws-region")
	backupCmd.MarkFlagRequired("aws-s3-bucket")
}
