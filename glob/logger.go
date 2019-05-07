package glob

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *GlobLogger

func initLogger(level string) {
	lvl := zap.NewAtomicLevel()
	loglevel := &lvl
	err := loglevel.UnmarshalText([]byte(level))
	if err != nil {
		fmt.Println(err)
	}
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = ""

	zapLogger := zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(encoderCfg), zapcore.Lock(os.Stdout), loglevel), zap.AddCaller())
	// defer zapLogger.Sync()

	Logger = &GlobLogger{zapLogger.Sugar()}
	Logger.SetName(ProjName)
	Logger.Info("Logger created.")
}

type GlobLogger struct {
	*zap.SugaredLogger
}

func (l *GlobLogger) SetName(name string) {
	Logger = &GlobLogger{l.Named(name)}
}
