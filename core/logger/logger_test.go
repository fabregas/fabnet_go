package logger

import (
	"testing"
)

func TestLogger(t *testing.T) {
	logger := NewLogger("node00")
	logger.Info("some string: %s", "hohoho")
	logger.Debug("debug int: %d", 34325)
	logger.Warning("some warn message")
	logger.Error("some fails: %d %s", 33, "network error")
}
