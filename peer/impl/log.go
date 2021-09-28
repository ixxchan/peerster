package impl

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// EnvLogLevel is the name of the environment variable to change the logging
// level.
//
//   LLVL=trace go test ./...
//   LLVL=info go test ./...
//
const EnvLogLevel = "LLVL"

const defaultLevel = zerolog.NoLevel

func init() {
	lvl := os.Getenv(EnvLogLevel)

	var level zerolog.Level

	switch lvl {
	case "error":
		level = zerolog.ErrorLevel
	case "warn":
		level = zerolog.WarnLevel
	case "info":
		level = zerolog.InfoLevel
	case "debug":
		level = zerolog.DebugLevel
	case "trace":
		level = zerolog.TraceLevel
	case "":
		level = defaultLevel
	default:
		level = zerolog.TraceLevel
	}

	Logger = Logger.Level(level)
}

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}

var Logger = zerolog.New(logout).Level(defaultLevel).
	With().Timestamp().Logger().
	With().Caller().Logger()
