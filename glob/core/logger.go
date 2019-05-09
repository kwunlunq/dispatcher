package core

import (
	"os"
	"go.uber.org/zap/zapcore"
	"log"
	"go.uber.org/zap"
)

var Logger *GlobLogger

func initLogger(level string, subScopName string) {
	lvl := zap.NewAtomicLevel()
	loglevel := &lvl
	err := loglevel.UnmarshalText([]byte(level))
	if err != nil {
		log.Println("Unknown log level: ", level)
		loglevel.SetLevel(zap.InfoLevel)
	}
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = ""

	zapLogger := zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(encoderCfg), zapcore.Lock(os.Stdout), loglevel), zap.AddCaller())

	Logger = &GlobLogger{zapLogger.Sugar()}
	Logger.SetName(subScopName)

	Logger.Debug("Logger created.")
}

type GlobLogger struct {
	*zap.SugaredLogger
}

func (l *GlobLogger) SetName(name string) {
	Logger = &GlobLogger{l.Named(name)}
}

func (l *GlobLogger) SafeExit() {
	l.Sync()
}
