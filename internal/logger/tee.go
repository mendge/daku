package logger

import (
	"io"
	"log"
	"os"
)

type Tee struct {
	Writer io.Writer
}

// 打开功能时，日志信息会同时输出到标准输出和Tee的写入器
func (l *Tee) Open() error {
	mw := io.MultiWriter(os.Stdout, l.Writer)
	log.SetOutput(mw)
	return nil
}

// 关闭功能时,日志信息只会输出到标准输出
func (l *Tee) Close() {
	log.SetOutput(os.Stdout)
}
