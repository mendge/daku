package cmd

import (
	"github.com/mendge/daku/app"
	"github.com/mendge/daku/internal/config"
	"github.com/mendge/daku/service/core"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func createSchedulerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scheduler",
		Short: "Start the scheduler",
		Long:  `dagu scheduler [--dags=<DAGs dir>]`,
		PreRun: func(cmd *cobra.Command, args []string) {
			cobra.CheckErr(config.LoadConfig())
		},
		Run: func(cmd *cobra.Command, args []string) {
			config.Get().DAGs = getFlagString(cmd, "dags", config.Get().DAGs)

			err := core.NewScheduler(app.TopLevelModule).Start(cmd.Context())
			checkError(err)
		},
	}
	cmd.Flags().StringP("dags", "d", "", "location of DAG files (default is $HOME/.dagu/dags)")
	_ = viper.BindPFlag("dags", cmd.Flags().Lookup("dags"))

	return cmd
}
