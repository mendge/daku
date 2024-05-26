package cmd

import (
	"github.com/mendge/daku/app"
	"github.com/mendge/daku/internal/config"
	"github.com/mendge/daku/service/core"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func startAllCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start-all",
		Short: "Launches both the Dagu web UI server and the scheduler process.",
		Long:  `dagu start-all [--dags=<DAGs dir>] [--host=<host>] [--port=<port>]`,
		PreRun: func(cmd *cobra.Command, args []string) {
			_ = viper.BindPFlag("port", cmd.Flags().Lookup("port"))
			_ = viper.BindPFlag("host", cmd.Flags().Lookup("host"))
			_ = viper.BindPFlag("dags", cmd.Flags().Lookup("dags"))
			cobra.CheckErr(config.LoadConfig())
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx := cmd.Context()

			go func() {
				config.Get().DAGs = getFlagString(cmd, "dags", config.Get().DAGs)
				err := core.NewScheduler(app.TopLevelModule).Start(cmd.Context())
				if err != nil {
					log.Fatal(err)
				}
			}()

			service := app.NewFrontendService()
			err := service.Start(ctx)
			checkError(err)
		},
	}
	bindStartAllCommandFlags(cmd)
	return cmd
}

func bindStartAllCommandFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("dags", "d", "", "location of DAG files (default is $HOME/.dagu/dags)")
	cmd.Flags().StringP("host", "s", "", "server host (default is localhost)")
	cmd.Flags().StringP("port", "p", "", "server port (default is 8080)")
}
