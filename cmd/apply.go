// Copyright © 2018 ThreeComma.io <hello@threecomma.io>

package cmd

import (
	"encoding/json"

	"github.com/spf13/afero"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/threecommaio/snappy/pkg/snappy"
)

var (
	node string
)

func init() {
	restoreCmd.AddCommand(applyCmd)
	applyCmd.Flags().StringVarP(&node, "node", "n", "", "the ip address of the destination node")
	applyCmd.MarkFlagRequired("node")
}

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Use:   "apply [mapping-file.json]",
	Short: "Load a mapping file and configure a destination node",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fs := afero.NewOsFs()
		if mappingFile, err := afero.ReadFile(fs, args[0]); err == nil {
			prepareMapping := &snappy.PrepareMapping{}
			if err := json.Unmarshal(mappingFile, &prepareMapping); err != nil {
				log.Fatal(err)
			}
			snappy.RestoreApply(fs, node, prepareMapping)
		} else {
			log.Fatal(err)
		}
	},
}
