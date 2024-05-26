package cmd

import (
	"github.com/mendge/daku/internal/agent"
	"github.com/mendge/daku/internal/config"
	"github.com/mendge/daku/internal/engine"
	"github.com/mendge/daku/internal/persistence/client"
	"github.com/spf13/cobra"
	"path/filepath"
)

func retryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry --req=<request-id> <DAG file>",
		Short: "Retry the DAG execution",
		Long:  `dagu retry --req=<request-id> <DAG file>`,
		Args:  cobra.ExactArgs(1),
		PreRun: func(cmd *cobra.Command, args []string) {
			cobra.CheckErr(config.LoadConfig())
		},
		Run: func(cmd *cobra.Command, args []string) {
			f, _ := filepath.Abs(args[0])
			reqID, err := cmd.Flags().GetString("req")
			checkError(err)

			// TODO: use engine.Engine instead of client.DataStoreFactory
			df := client.NewDataStoreFactory(config.Get())
			e := engine.NewFactory(df, nil).Create()

			hs := df.NewHistoryStore()

			status, err := hs.FindByRequestId(f, reqID)
			checkError(err)

			loadedDAG, err := loadDAG(args[0], status.Status.Params)
			checkError(err)

			a := agent.New(&agent.Config{DAG: loadedDAG, RetryTarget: status.Status}, e, df)
			ctx := cmd.Context()
			listenSignals(ctx, a)
			checkError(a.Run(ctx))
		},
	}
	cmd.Flags().StringP("req", "r", "", "request-id")
	_ = cmd.MarkFlagRequired("req")
	return cmd
}
