package core

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"time"
)

var Logger *GlobLogger

func InitLogger(level string, subScopName string) {
	lvl := zap.NewAtomicLevel()
	loglevel := &lvl
	err := loglevel.UnmarshalText([]byte(level))
	if err != nil {
		log.Println("Unknown log level: ", level)
		loglevel.SetLevel(zap.InfoLevel)
	}
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.TimeEncoder(func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.UTC().Format("2006-01-02 15:04:05"))
		// 2019-08-13T04:39:11Z
	})
	//encoderCfg.TimeKey = "t"

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
