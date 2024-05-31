package agent

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	config2 "github.com/mendge/daku/internal/config"
	"github.com/mendge/daku/internal/constants"
	"github.com/mendge/daku/internal/dag"
	"github.com/mendge/daku/internal/engine"
	"github.com/mendge/daku/internal/etcd/estore"
	"github.com/mendge/daku/internal/etcd/runtime"
	"github.com/mendge/daku/internal/logger"
	"github.com/mendge/daku/internal/mailer"
	"github.com/mendge/daku/internal/pb"
	"github.com/mendge/daku/internal/persistence"
	"github.com/mendge/daku/internal/persistence/model"
	"github.com/mendge/daku/internal/reporter"
	"github.com/mendge/daku/internal/scheduler"
	"github.com/mendge/daku/internal/utils"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// Agent is the interface to run / cancel / signal / status / etc.
type Agent struct {
	*Config

	// TODO: Do not use the persistence package directly.
	dataStoreFactory persistence.DataStoreFactory
	engine           engine.Engine
	scheduler        *scheduler.Scheduler
	graph            *scheduler.ExecutionGraph
	logManager       *logManager
	reporter         *reporter.Reporter
	historyStore     persistence.HistoryStore
	runtimeServer    *runtime.RuntimeServer
	requestId        string
	finished         uint32
}

func New(config *Config, e engine.Engine, ds persistence.DataStoreFactory) *Agent {
	return &Agent{
		Config:           config,
		engine:           e,
		dataStoreFactory: ds,
	}
}

// Config contains the configuration for an Agent.
type Config struct {
	DAG *dag.DAG
	Dry bool

	// RetryTarget is the status to retry.
	RetryTarget *model.Status
}

// Run starts the dags execution.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.setupRequestId(); err != nil {
		return err
	}
	a.init()
	if err := a.setupGraph(); err != nil {
		return err
	}
	if err := a.checkPreconditions(); err != nil {
		return err
	}
	if a.Dry {
		return a.dryRun()
	}
	setup := []func() error{
		a.checkIsRunning,
		a.setupDatabase,
		a.setupSocketServer,
		a.logManager.setupLogFile,
	}
	for _, fn := range setup {
		err := fn()
		if err != nil {
			return err
		}
	}
	return a.run(ctx)
}

// Status returns the current status of the dags.
func (a *Agent) Status() *model.Status {
	scStatus := a.scheduler.Status(a.graph)
	if scStatus == scheduler.SchedulerStatus_None && !a.graph.StartedAt.IsZero() {
		scStatus = scheduler.SchedulerStatus_Running
	}

	status := model.NewStatus(
		a.DAG,
		a.graph.Nodes(),
		scStatus,
		os.Getpid(),
		&a.graph.StartedAt,
		&a.graph.FinishedAt,
	)
	status.RequestId = a.requestId
	// modified 运行时生成状态时，将本地的日志存储路径换为etcd中的路径
	status.Log = a.logManager.remoteScLogPath
	if node := a.scheduler.HandlerNode(constants.OnExit); node != nil {
		status.OnExit = model.FromNode(node)
	}
	if node := a.scheduler.HandlerNode(constants.OnSuccess); node != nil {
		status.OnSuccess = model.FromNode(node)
	}
	if node := a.scheduler.HandlerNode(constants.OnFailure); node != nil {
		status.OnFailure = model.FromNode(node)
	}
	if node := a.scheduler.HandlerNode(constants.OnCancel); node != nil {
		status.OnCancel = model.FromNode(node)
	}
	return status
}

// Signal sends the signal to the processes running
// if processes do not terminate after MaxCleanUp time, it will send KILL signal.
func (a *Agent) Signal(sig os.Signal) {
	a.signal(sig, false)
}

// Kill sends KILL signal to all child processes.
func (a *Agent) Kill() {
	log.Printf("Sending KILL signal to running child processes.")
	a.scheduler.Signal(a.graph, syscall.SIGKILL, nil, false)
}

func (a *Agent) signal(sig os.Signal, allowOverride bool) {
	log.Printf("Sending %s signal to running child processes.", sig)
	done := make(chan bool)
	go func() {
		a.scheduler.Signal(a.graph, sig, done, allowOverride)
	}()
	timeout := time.After(a.DAG.MaxCleanUpTime)
	tick := time.After(time.Second * 5)
	for {
		select {
		case <-done:
			log.Printf("All child processes have been terminated.")
			return
		case <-timeout:
			log.Printf("Time reached to max cleanup time")
			a.Kill()
			return
		case <-tick:
			log.Printf("Sending signal again")
			a.scheduler.Signal(a.graph, sig, nil, false)
			tick = time.After(time.Second * 5)
		default:
			log.Printf("Waiting for child processes to exit...")
			time.Sleep(time.Second * 3)
		}
	}
}

func (a *Agent) init() {
	localLogDir := path.Join(config2.Get().TmpLogDir, utils.ValidFilename(a.DAG.Name, "_"))
	config := &scheduler.Config{
		LogDir:        localLogDir,
		MaxActiveRuns: a.DAG.MaxActiveRuns,
		Delay:         a.DAG.Delay,
		Dry:           a.Dry,
		RequestId:     a.requestId,
	}

	if a.DAG.HandlerOn.Exit != nil {
		onExit, _ := pb.ToPbStep(a.DAG.HandlerOn.Exit)
		config.OnExit = onExit
	}

	if a.DAG.HandlerOn.Success != nil {
		onSuccess, _ := pb.ToPbStep(a.DAG.HandlerOn.Success)
		config.OnSuccess = onSuccess
	}

	if a.DAG.HandlerOn.Failure != nil {
		onFailure, _ := pb.ToPbStep(a.DAG.HandlerOn.Failure)
		config.OnFailure = onFailure
	}

	if a.DAG.HandlerOn.Cancel != nil {
		onCancel, _ := pb.ToPbStep(a.DAG.HandlerOn.Cancel)
		config.OnCancel = onCancel
	}

	a.scheduler = &scheduler.Scheduler{
		Config: config,
	}
	a.reporter = &reporter.Reporter{
		Config: &reporter.Config{
			Mailer: &mailer.Mailer{
				Config: &mailer.Config{
					Host:     a.DAG.Smtp.Host,
					Port:     a.DAG.Smtp.Port,
					Username: a.DAG.Smtp.Username,
					Password: a.DAG.Smtp.Password,
				},
			},
		}}
	timestamp := fmt.Sprintf("%s.%s",
		time.Now().Format("20060102.15:04:05.000"), utils.TruncString(a.requestId, 8))
	remoteLogDir := filepath.Join(a.DAG.LogDir, utils.ValidFilename(a.DAG.Name, "_"))
	a.logManager = newLogManager(localLogDir, remoteLogDir, timestamp, a.DAG)
}

func (a *Agent) setupGraph() (err error) {
	if a.RetryTarget != nil {
		log.Printf("setup for retry")
		return a.setupRetry()
	}
	a.graph, err = scheduler.NewExecutionGraph(a.DAG.Steps...)
	return
}

func (a *Agent) setupRetry() (err error) {
	nodes := make([]*scheduler.Node, 0, len(a.RetryTarget.Nodes))
	for _, n := range a.RetryTarget.Nodes {
		nodes = append(nodes, n.ToNode())
	}
	a.graph, err = scheduler.NewExecutionGraphForRetry(nodes...)
	return
}

func (a *Agent) setupRequestId() error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	a.requestId = id.String()
	return nil
}

func (a *Agent) setupDatabase() error {
	// TODO: do not use the persistence package directly.
	a.historyStore = a.dataStoreFactory.NewHistoryStore()
	if err := a.historyStore.RemoveOld(a.DAG.Location, a.DAG.HistRetentionDays); err != nil {
		utils.LogErr("clean old history data", err)
	}
	if err := a.historyStore.Open(a.DAG.Location, time.Now(), a.requestId); err != nil {
		return err
	}
	return nil
}

func (a *Agent) setupSocketServer() (err error) {
	a.runtimeServer = runtime.NewRuntimeServer(a.DAG.SockAddr(), a.Handle)
	return
}

func (a *Agent) checkPreconditions() error {
	if len(a.DAG.Preconditions) > 0 {
		log.Printf("checking preconditions for \"%s\"", a.DAG.Name)
		if err := dag.EvalConditions(a.DAG.Preconditions); err != nil {
			a.scheduler.Cancel(a.graph)
			return err
		}
	}
	return nil
}

func (a *Agent) run(ctx context.Context) error {
	tl := &logger.Tee{Writer: a.logManager.logFile}
	if err := tl.Open(); err != nil {
		return err
	}
	defer func() {
		utils.LogErr("uploadLog to etcd", a.logManager.uploadLog(a.graph))
		utils.LogErr("close log file", a.closeLogFile())
		tl.Close()
	}()

	defer func() {
		if err := a.historyStore.Close(); err != nil {
			log.Printf("failed to close history store: %v", err)
		}
	}()

	utils.LogErr("write initial status", a.historyStore.Write(a.Status()))

	go func() {
		a.runtimeServer.Serve()
	}()

	defer func() {
		utils.LogErr("shutdown socket frontend", a.runtimeServer.Shutdown())
	}()

	done := make(chan *scheduler.Node)
	defer close(done)

	go func() {
		for node := range done {
			status := a.Status()
			utils.LogErr("write done status", a.historyStore.Write(status))
			utils.LogErr("report step", a.reporter.ReportStep(a.DAG, status, node))
		}
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		if a.finished == 1 {
			return
		}
		utils.LogErr("write finished status", a.historyStore.Write(a.Status()))
	}()

	ctx = dag.NewContext(ctx, a.DAG)

	lastErr := a.scheduler.Schedule(ctx, a.graph, done)
	status := a.Status()

	log.Println("schedule finished.")
	lastStatus := a.Status()
	lastStatus.MakeNodesLogPathRemote(a.logManager.localLogDir, a.logManager.remoteLogDir)
	utils.LogErr("write last status", a.historyStore.Write(lastStatus))

	a.reporter.ReportSummary(status, lastErr)
	utils.LogErr("send email", a.reporter.SendMail(a.DAG, status, lastErr))

	atomic.CompareAndSwapUint32(&a.finished, 0, 1)

	return lastErr
}

func (a *Agent) dryRun() error {
	done := make(chan *scheduler.Node)
	defer func() {
		close(done)
	}()

	go func() {
		for node := range done {
			status := a.Status()
			_ = a.reporter.ReportStep(a.DAG, status, node)
		}
	}()

	log.Printf("***** Starting DRY-RUN *****")

	ctx := dag.NewContext(context.Background(), a.DAG)

	lastErr := a.scheduler.Schedule(ctx, a.graph, done)
	status := a.Status()
	a.reporter.ReportSummary(status, lastErr)

	log.Printf("***** Finished DRY-RUN *****")

	return lastErr
}

func (a *Agent) checkIsRunning() error {
	status, err := a.engine.GetCurrentStatus(a.DAG)
	if err != nil {
		return err
	}
	if status.Status != scheduler.SchedulerStatus_None {
		return fmt.Errorf("the DAG is already running. socket=%s",
			a.DAG.SockAddr())
	}
	return nil
}

func (a *Agent) closeLogFile() error {
	if a.logManager.logFile != nil {
		return a.logManager.logFile.Close()
	}
	return nil
}

var (
	statusRe = regexp.MustCompile(`^/status[/]?$`)
	stopRe   = regexp.MustCompile(`^/stop[/]?$`)
)

func (a *Agent) Handle(server *runtime.RuntimeServer, event *runtime.Event) {
	if event.Type == runtime.ReqStatus {
		status := a.Status()
		status.Status = scheduler.SchedulerStatus_Running
		b, err := status.ToJson()
		if err != nil {
			event.Type = runtime.RespErr
			event.Data = err.Error()
			server.Write(event)
			return
		}
		event.Type = runtime.Response
		event.Data = string(b)
		server.Write(event)

	} else if event.Type == runtime.ReqStop {
		event.Type = runtime.Response
		event.Data = "OK"
		server.Write(event)
		go func() {
			log.Printf("stop request received. shutting down...")
			a.signal(syscall.SIGTERM, true)
		}()
	}
}

type logManager struct {
	timestamp string
	// local scheduler log path（历史原因，保留原格式名字）
	logFilename     string
	remoteScLogPath string
	localLogDir     string
	remoteLogDir    string
	logFile         *os.File
}

func newLogManager(lLogDir string, rLogDer string, timestamp string, d *dag.DAG) *logManager {
	scLogName := fmt.Sprintf("agent_%s.%s.log", utils.ValidFilename(d.Name, "_"), timestamp)
	localLogPath := path.Join(lLogDir, scLogName)
	remoteLogPath := path.Join(rLogDer, scLogName)
	return &logManager{
		timestamp:       timestamp,
		logFilename:     localLogPath,
		remoteScLogPath: remoteLogPath,
		localLogDir:     lLogDir,
		remoteLogDir:    rLogDer,
	}
}

func (l *logManager) setupLogFile() (err error) {
	dir := path.Dir(l.logFilename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	l.logFile, err = utils.OpenOrCreateFile(l.logFilename)
	return
}

func (l *logManager) uploadLog(graph *scheduler.ExecutionGraph) error {
	data, err := os.ReadFile(l.logFilename)
	if err != nil {
		return err
	}
	err = estore.SaveFile(l.remoteScLogPath, string(data))
	if err != nil {
		return err
	}
	for _, node := range graph.Nodes() {
		if node.IsDone() {
			localStepLogPath := node.Log
			data, err := os.ReadFile(localStepLogPath)
			if err != nil {
				return err
			}
			remoteStepLogPath := strings.Replace(localStepLogPath, l.localLogDir, l.remoteLogDir, 1)
			err = estore.SaveFile(remoteStepLogPath, string(data))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type HTTPError struct {
	Code    int
	Message string
}

func (e *HTTPError) Error() string {
	return e.Message
}

func encodeError(w http.ResponseWriter, err error) {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		http.Error(w, httpErr.Error(), httpErr.Code)
	} else {
		http.Error(w, httpErr.Error(), http.StatusInternalServerError)
	}
}
