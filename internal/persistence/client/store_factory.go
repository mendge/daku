package client

import (
	"github.com/mendge/daku/internal/config"
	"github.com/mendge/daku/internal/persistence"
	"github.com/mendge/daku/internal/persistence/jsondb"
	"github.com/mendge/daku/internal/persistence/local"
	"github.com/mendge/daku/internal/persistence/local/storage"
	"os"
)

type dataStoreFactoryImpl struct {
	cfg *config.Config
}

var _ persistence.DataStoreFactory = (*dataStoreFactoryImpl)(nil)

func NewDataStoreFactory(cfg *config.Config) persistence.DataStoreFactory {
	ds := &dataStoreFactoryImpl{
		cfg: cfg,
	}
	_ = ds.InitDagDir()
	return ds
}

func (f dataStoreFactoryImpl) InitDagDir() error {
	_, err := os.Stat(f.cfg.DAGs)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(f.cfg.DAGs, 0755); err != nil {
			return err
		}
	}

	return nil
}

func (f dataStoreFactoryImpl) NewHistoryStore() persistence.HistoryStore {
	// TODO: Add support for other data stores (e.g. sqlite, postgres, etc.)
	return jsondb.New(f.cfg.DataDir, f.cfg.DAGs)
}

func (f dataStoreFactoryImpl) NewDAGStore() persistence.DAGStore {
	return local.NewDAGStore(f.cfg.DAGs)
}

func (f dataStoreFactoryImpl) NewFlagStore() persistence.FlagStore {
	s := storage.NewStorage(f.cfg.SuspendFlagsDir)
	return local.NewFlagStore(s)
}
