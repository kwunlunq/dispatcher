package glob

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *globLogger

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
	defer zapLogger.Sync()

	Logger = &globLogger{zapLogger.Sugar()}
	Logger.Info("Logger constructed.")
}

type globLogger struct {
	*zap.SugaredLogger
}

func (l *globLogger) SetName(name string) {
	Logger = &globLogger{l.Named(name)}
}
