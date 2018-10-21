// Copyright © 2018 ThreeComma.io <hello@threecomma.io>

package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/threecommaio/snappy/pkg/snappy"
)

// backupCmd represents the backup command
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Creates a snapshot and uploads to an S3 bucket",
	Run: func(cmd *cobra.Command, args []string) {
		var (
			region, _        = cmd.Flags().GetString("region")
			bucket, _        = cmd.Flags().GetString("bucket")
			cloudProvider, _ = cmd.Flags().GetString("cloud-provider")
			throttle, _      = cmd.Flags().GetInt("throttle")
			snapshotID, _    = cmd.Flags().GetString("snapshot-id")
			keyspaces, _     = cmd.Flags().GetStringSlice("keyspaces")
			cp               snappy.CloudProvider
		)
		switch cloudProvider {
		case "aws":
			cp = snappy.CloudProviderAWS
		case "gcp":
			cp = snappy.CloudProviderGCP
		default:
			log.Fatal("unsupported cloud provider")
		}
		config := &snappy.CloudConfig{Provider: cp, Bucket: bucket, Region: region, Throttle: throttle}
		snappy.Backup(config, snapshotID, keyspaces)
	},
}

func init() {
	rootCmd.AddCommand(backupCmd)

	backupCmd.Flags().StringP("snapshot-id", "s", "", "snapshot id")
	backupCmd.Flags().StringP("region", "r", "", "the region of the cloud provider")
	backupCmd.Flags().StringP("bucket", "b", "", "the bucket name of the cloud provider")
	backupCmd.Flags().String("cloud-provider", "aws", "cloud provider name (aws or gcp)")
	backupCmd.Flags().IntP("throttle", "t", 200, "throttle in megabits/s")
	backupCmd.Flags().StringSliceP("keyspaces", "k", []string{}, "include only these keyspaces")

	backupCmd.MarkFlagRequired("snapshot-id")
	backupCmd.MarkFlagRequired("region")
	backupCmd.MarkFlagRequired("bucket")
}
