package dag

import (
	"crypto/md5"
	"fmt"
	"github.com/mendge/daku/internal/config"
	"github.com/robfig/cron/v3"
	"os"
	"path"
	"strings"
	"time"
)

// DAG represents a DAG configuration.
type DAG struct {
	Location          string
	Group             string
	Name              string
	Schedule          []*Schedule
	StopSchedule      []*Schedule
	RestartSchedule   []*Schedule
	Description       string
	Env               []string
	LogDir            string
	HandlerOn         HandlerOn
	Steps             []*Step
	MailOn            *MailOn
	ErrorMail         *MailConfig
	InfoMail          *MailConfig
	Smtp              *SmtpConfig
	Delay             time.Duration
	RestartWait       time.Duration
	HistRetentionDays int
	Preconditions     []*Condition
	MaxActiveRuns     int
	Params            []string
	DefaultParams     string
	MaxCleanUpTime    time.Duration
	Tags              []string
}

type Schedule struct {
	Expression string
	Parsed     cron.Schedule
}

type HandlerOn struct {
	Failure *Step
	Success *Step
	Cancel  *Step
	Exit    *Step
}

type MailOn struct {
	Failure bool
	Success bool
}

func ReadFile(file string) (string, error) {
	b, err := os.ReadFile(file)
	return string(b), err
}

func (d *DAG) HasTag(tag string) bool {
	for _, t := range d.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

func (d *DAG) SockAddr() string {
	s := strings.ReplaceAll(d.Location, " ", "_")
	name := strings.Replace(path.Base(s), path.Ext(path.Base(s)), "", 1)
	h := md5.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)
	return path.Join("/tmp", fmt.Sprintf("@dagu-%s-%x.sock", name, bs))
}

func (d *DAG) Clone() *DAG {
	ret := *d
	return &ret
}

func (d *DAG) String() string {
	ret := "{\n"
	ret = fmt.Sprintf("%s\tName: %s\n", ret, d.Name)
	ret = fmt.Sprintf("%s\tDescription: %s\n", ret, strings.TrimSpace(d.Description))
	ret = fmt.Sprintf("%s\tEnv: %v\n", ret, strings.Join(d.Env, ", "))
	ret = fmt.Sprintf("%s\tLogDir: %v\n", ret, d.LogDir)
	for i, s := range d.Steps {
		ret = fmt.Sprintf("%s\tStep%d: %v\n", ret, i, s)
	}
	ret = fmt.Sprintf("%s}\n", ret)
	return ret
}

// 配置dag的steps\handlers\其他
func (d *DAG) setup() {
	d.setDefaults()
	d.setupSteps()
	d.setupHandlers()
}

func (d *DAG) setDefaults() {
	if d.LogDir == "" {
		d.LogDir = config.Get().LogDir
	}
	if d.HistRetentionDays == 0 {
		d.HistRetentionDays = 30
	}
	if d.MaxCleanUpTime == 0 {
		d.MaxCleanUpTime = time.Second * 60
	}
	if d.Env == nil {
		d.Env = []string{}
	}
	if d.Steps == nil {
		d.Steps = []*Step{}
	}
	if d.Params == nil {
		d.Params = []string{}
	}
	if d.Preconditions == nil {
		d.Preconditions = []*Condition{}
	}
}

func (d *DAG) setupHandlers() {
	for _, handlerStep := range []*Step{
		d.HandlerOn.Exit,
		d.HandlerOn.Success,
		d.HandlerOn.Failure,
		d.HandlerOn.Cancel,
	} {
		if handlerStep != nil {
			handlerStep.setup(d.Name)
		}
	}
}

func (d *DAG) setupSteps() {
	for _, step := range d.Steps {
		step.setup(d.Name)
	}
}
