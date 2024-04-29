package dag

import (
	"crypto/md5"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/dagu-dev/dagu/internal/config"
	"github.com/robfig/cron/v3"
)

// DAG represents a DAG configuration.
type DAG struct {
	Location string
	Group    string
	Name     string
	// 三种操作 的定时调度描述(表达式和具体的Cron.Schedule)
	Schedule        []*Schedule
	StopSchedule    []*Schedule
	RestartSchedule []*Schedule
	Description     string
	// "k=v"的形式
	Env       []string
	LogDir    string
	HandlerOn HandlerOn
	Steps     []*Step
	// 标记成功后和失败 是否有邮件通知
	MailOn    *MailOn
	ErrorMail *MailConfig
	InfoMail  *MailConfig
	Smtp      *SmtpConfig

	Delay             time.Duration
	RestartWait       time.Duration
	HistRetentionDays int
	Preconditions     []*Condition
	MaxActiveRuns     int

	Params        []string
	DefaultParams string
	// ??
	MaxCleanUpTime time.Duration
	Tags           []string
}

// dag中的定时调度描述
type Schedule struct {
	Expression string
	Parsed     cron.Schedule
}

// ?? dag用于处理各种结束状态的Step
type HandlerOn struct {
	Failure *Step
	Success *Step
	Cancel  *Step
	Exit    *Step
}

// 成功后和失败是否有邮件通知, 用于标记
type MailOn struct {
	Failure bool
	Success bool
}

// 将文件内容以字符串形式返回
func ReadFile(file string) (string, error) {
	b, err := os.ReadFile(file)
	return string(b), err
}

// 判断当前dag是否有Tag
func (d *DAG) HasTag(tag string) bool {
	for _, t := range d.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// 给当前dag返回一个.sock文件路径(在/tmp下, 命名根据dag来)
func (d *DAG) SockAddr() string {
	s := strings.ReplaceAll(d.Location, " ", "_")
	name := strings.Replace(path.Base(s), path.Ext(path.Base(s)), "", 1)
	h := md5.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)
	return path.Join("/tmp", fmt.Sprintf("@dagu-%s-%x.sock", name, bs))
}

// 建立一个同当前dag一模一样的DAG, 返回指针
func (d *DAG) Clone() *DAG {
	ret := *d
	return &ret
}

// 将当前dag的粗略信息作为string返回
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

// 传递dag的文件文件给其所有step, 初始化特殊类型成员变量
func (d *DAG) setup() {
	d.setDefaults()
	d.setupSteps()
	d.setupHandlers()
}

// 对dag进行一些基础的配置 && 初始化dag的map和slice类型成员变量
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

// 传递当前dag的文件位置给dag各个handler对应的step
func (d *DAG) setupHandlers() {
	dir := path.Dir(d.Location)
	for _, handlerStep := range []*Step{
		d.HandlerOn.Exit,
		d.HandlerOn.Success,
		d.HandlerOn.Failure,
		d.HandlerOn.Cancel,
	} {
		if handlerStep != nil {
			handlerStep.setup(dir)
		}
	}
}

// 将当前dag的所属文件位置传递给其所有的step
func (d *DAG) setupSteps() {
	dir := path.Dir(d.Location)
	for _, step := range d.Steps {
		step.setup(dir)
	}
}
