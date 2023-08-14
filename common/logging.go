package common

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func GetLogger(debug, prod bool) *zap.SugaredLogger {
	var logger *zap.Logger
	zapLevel := zap.NewAtomicLevel()
	if debug {
		zapLevel.SetLevel(zap.DebugLevel)
	}
	if prod {
		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		logger = zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			zapLevel,
		))
	} else {
		logger = zap.New(zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
			zapcore.Lock(os.Stdout),
			zapLevel,
		))
	}
	return logger.Sugar()
}
