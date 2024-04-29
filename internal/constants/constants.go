package constants

var (
	Version = "0.0.1"
)

// 任务节点的执行状态
const (
	OnSuccess = "onSuccess"
	OnFailure = "onFailure"
	OnCancel  = "onCancel"
	OnExit    = "onExit"
)

const (
	TimeFormat = "2006-01-02 15:04:05"
	TimeEmpty  = "-"
)
