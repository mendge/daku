package scheduler

import (
	"context"
	"github.com/mendge/daku/internal/dag"
	"github.com/mendge/daku/internal/logger"
	"github.com/mendge/daku/internal/utils"
	"github.com/mendge/daku/service/distribution"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
)

type Scheduler struct {
	serverNode  *distribution.ServerNode
	entryReader EntryReader
	logDir      string
	stop        chan struct{}
	running     bool
	logger      logger.Logger
}

type EntryReader interface {
	Read(now time.Time) ([]*Entry, error)
	LoadDags(serverName string) error
	LogErr(msg string, err error)
}

type Entry struct {
	Next      time.Time
	Job       Job
	EntryType Type
	Logger    logger.Logger
}

type Job interface {
	GetDAG() *dag.DAG
	Start() error
	Stop() error
	Restart() error
	String() string
}

type Type int

const (
	Start Type = iota
	Stop
	Restart
)

func (e *Entry) Invoke() error {
	if e.Job == nil {
		return nil
	}
	switch e.EntryType {
	case Start:
		e.Logger.Info("start job", "job", e.Job.String(), "time", e.Next.Format("2006-01-02 15:04:05"))
		return e.Job.Start()
	case Stop:
		e.Logger.Info("stop job", "job", e.Job.String(), "time", e.Next.Format("2006-01-02 15:04:05"))
		return e.Job.Stop()
	case Restart:
		e.Logger.Info("restart job", "job", e.Job.String(), "time", e.Next.Format("2006-01-02 15:04:05"))
		return e.Job.Restart()
	}
	return nil
}

type Params struct {
	EntryReader EntryReader
	Logger      logger.Logger
	LogDir      string
}

func New(params Params) *Scheduler {
	return &Scheduler{
		entryReader: params.EntryReader,
		logDir:      params.LogDir,
		stop:        make(chan struct{}),
		running:     false,
		logger:      params.Logger,
	}
}

func (s *Scheduler) Start() error {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sig
		s.Stop()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startDistributedManage(ctx)

	s.logger.Info("starting scheduler")
	s.start()

	return nil
}

func (s *Scheduler) startDistributedManage(ctx context.Context) {
	s.serverNode = distribution.GetServerNode()

	sem := make(chan struct{}, 1)
	sem0 := make(chan struct{}, 1)
	go s.serverNode.WatchCronmap(ctx, sem, sem0)
	go func(sem <-chan struct{}) {
		for {
			select {
			case <-sem:
				if err := s.entryReader.LoadDags(s.serverNode.ServerName); err != nil {
					s.entryReader.LogErr("failed to reload entry_reader dags", err)
				}
			}
		}
	}(sem)
	//time.Sleep(1500 * time.Millisecond)
	<-sem0
	go s.serverNode.Campaign(ctx)
	//time.Sleep(1500 * time.Millisecond)
	sem1 := make(chan struct{}, 1)
	go s.serverNode.WatchHealth(ctx, sem1)
	//time.Sleep(1500 * time.Millisecond)
	<-sem1
	go s.serverNode.KeepAlive(ctx)
	//time.Sleep(1500 * time.Millisecond)
	go s.serverNode.WatchDags(ctx)
}

func (s *Scheduler) start() {
	t := utils.Now().Truncate(time.Second * 60)
	timer := time.NewTimer(0)
	s.running = true
	for {
		select {
		case <-timer.C:
			s.run(t)
			t = s.nextTick(t)
			timer = time.NewTimer(t.Sub(utils.Now()))
		case <-s.stop:
			_ = timer.Stop()
			return
		}
	}
}

func (s *Scheduler) run(now time.Time) {
	entries, err := s.entryReader.Read(now.Add(-time.Second))
	utils.LogErr("failed to read entries", err)
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Next.Before(entries[j].Next)
	})
	for _, e := range entries {
		t := e.Next
		if t.After(now) {
			break
		}
		go func(e *Entry) {
			err := e.Invoke()
			if err != nil {
				s.logger.Error("failed to invoke entry_reader", "entry_reader", e.Job, "error", err)
			}
		}(e)
	}
}

func (s *Scheduler) nextTick(now time.Time) time.Time {
	return now.Add(time.Minute).Truncate(time.Second * 60)
}

func (s *Scheduler) Stop() {
	if !s.running {
		return
	}
	if s.stop != nil {
		s.stop <- struct{}{}
	}
	s.running = false
}
