// Copyright © 2018 ThreeComma.io <hello@threecomma.io>

package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/afero"

	"github.com/spf13/cobra"
	"github.com/threecommaio/snappy/pkg/snappy"
)

var (
	clusterName string
	srcNodes    []string
	dstNodes    []string
)

func init() {
	restoreCmd.AddCommand(prepareCmd)

	prepareCmd.Flags().StringVarP(&clusterName, "cluster", "c", "", "cluster name")
	prepareCmd.Flags().StringSliceVarP(&srcNodes, "srcNodes", "s", []string{}, "list of source node ip addresses")
	prepareCmd.Flags().StringSliceVarP(&dstNodes, "dstNodes", "d", []string{}, "list of destination node ip addresses")
	prepareCmd.MarkFlagRequired("clusterName")
	prepareCmd.MarkFlagRequired("srcNodes")
	prepareCmd.MarkFlagRequired("dstNodes")
}

// prepareCmd represents the prepare command
var prepareCmd = &cobra.Command{
	Use:   "prepare",
	Short: "Creates a mapping file from old to new nodes",
	Run: func(cmd *cobra.Command, args []string) {
		fs := afero.NewOsFs()
		prepareConfig := &snappy.PrepareConfig{
			ClusterName:      clusterName,
			SourceNodes:      srcNodes,
			DestinationNodes: dstNodes,
		}
		prepareJSON, err := snappy.RestorePrepare(fs, prepareConfig)
		if err != nil {
			log.Fatal(err)
		}
		mappingFilename := fmt.Sprintf("%s-mapping.json", clusterName)
		err = afero.WriteFile(fs, mappingFilename, prepareJSON, 0644)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("successfully created mapping file: %s\n", mappingFilename)
	},
}
