package app

import (
	"github.com/mendge/daku/internal/config"
	"github.com/mendge/daku/internal/engine"
	"github.com/mendge/daku/internal/logger"
	"github.com/mendge/daku/internal/persistence/client"
	"github.com/mendge/daku/service/frontend"
	"go.uber.org/fx"
)

var (
	TopLevelModule = fx.Options(
		fx.Provide(ConfigProvider),
		fx.Provide(engine.NewFactory),
		fx.Provide(logger.NewSlogLogger),
		fx.Provide(client.NewDataStoreFactory),
	)
)

var (
	cfgInstance *config.Config = nil
)

func ConfigProvider() *config.Config {
	if cfgInstance != nil {
		return cfgInstance
	}
	if err := config.LoadConfig(); err != nil {
		panic(err)
	}
	cfgInstance = config.Get()
	return cfgInstance
}

func NewFrontendService() *fx.App {
	return fx.New(
		TopLevelModule,
		frontend.Module,
		fx.Invoke(frontend.LifetimeHooks),
	)
}
