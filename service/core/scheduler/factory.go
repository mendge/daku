package scheduler

import (
	"github.com/mendge/daku/internal/dag"
	"github.com/mendge/daku/internal/engine"
	"github.com/mendge/daku/service/core/scheduler/job"
	"github.com/mendge/daku/service/core/scheduler/scheduler"
	"time"
)

type jobFactory struct {
	Command       string
	WorkDir       string
	EngineFactory engine.Factory
}

func (jf jobFactory) NewJob(dag *dag.DAG, next time.Time) scheduler.Job {
	return &job.Job{
		DAG:           dag,
		Command:       jf.Command,
		WorkDir:       jf.WorkDir,
		Next:          next,
		EngineFactory: jf.EngineFactory,
	}
}
