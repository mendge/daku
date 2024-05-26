package cmd

import (
	"github.com/mendge/daku/internal/config"
	"github.com/mendge/daku/internal/engine"
	"github.com/mendge/daku/internal/persistence/client"
	"github.com/spf13/cobra"
	"log"
)

func stopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop <DAG file>",
		Short: "Stop the running DAG",
		Long:  `dagu stop <DAG file>`,
		Args:  cobra.ExactArgs(1),
		PreRun: func(cmd *cobra.Command, args []string) {
			cobra.CheckErr(config.LoadConfig())
		},
		Run: func(cmd *cobra.Command, args []string) {
			loadedDAG, err := loadDAG(args[0], "")
			checkError(err)

			log.Printf("Stopping...")

			df := client.NewDataStoreFactory(config.Get())
			e := engine.NewFactory(df, config.Get()).Create()
			checkError(e.Stop(loadedDAG))
		},
	}
}
