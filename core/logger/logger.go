package logger

import (
	"fmt"
	"time"
)

type Logger struct {
	object string
}

func NewLogger(object string) *Logger {
	return &Logger{object}
}

func (l *Logger) nowLogFormat(format, ll string) string {
	return time.Now().Format("15:04:05.0000") + " " + l.object + " " + ll + " " + format + "\n"
}

func (l *Logger) Debug(format string, args ...interface{}) {
	fmt.Printf(l.nowLogFormat(format, "DEBU"), args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	fmt.Printf(l.nowLogFormat(format, "INFO"), args...)
}

func (l *Logger) Warning(format string, args ...interface{}) {
	fmt.Printf(l.nowLogFormat(format, "WARN"), args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	fmt.Printf(l.nowLogFormat(format, "ERRO"), args...)
}
