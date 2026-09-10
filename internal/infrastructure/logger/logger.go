// Package logger provides the application-wide zerolog-based structured logging
// infrastructure. It is the single place that owns the zerolog implementation;
// every other layer (inbound adapters, application services, outbound adapters)
// only consumes a logger extracted from context, never constructs one directly.
package logger

import (
	"context"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"github.com/rs/zerolog"
)

// ctxKey is the unexported key type used to store the logger in a context.
type ctxKey struct{}

// base is the package-level fallback logger, initialised by Init and returned
// by FromContext when no logger has been stored in a context. It is held in an
// atomic pointer so a late Init (a test harness, a config reload) can never
// race a concurrent read on the delivery path.
var base atomic.Pointer[zerolog.Logger]

func init() {
	nop := zerolog.Nop()
	base.Store(&nop)
}

// Init configures zerolog global defaults and initialises the package-level
// base logger. It must be called exactly once at the composition root before
// any goroutines are started.
//
// level is a case-insensitive zerolog level name: trace, debug, info, warn,
// error, fatal, panic. An empty or unrecognised value defaults to info.
func Init(level string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = erisStackMarshaler

	lvl, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil || level == "" {
		lvl = zerolog.InfoLevel
	}

	// Embed build-time metadata as static fields on every log entry.
	var revision, goVer string
	if info, ok := debug.ReadBuildInfo(); ok {
		goVer = info.GoVersion
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				revision = s.Value
			}
		}
	}

	logger := zerolog.New(os.Stderr).
		Level(lvl).
		With().
		Timestamp().
		Caller().
		Str("go_version", goVer).
		Str("git_revision", revision).
		Logger()
	base.Store(&logger)

	return logger
}

// Base returns the package-level logger initialised by Init.
func Base() *zerolog.Logger { return base.Load() }

// Context returns a copy of ctx containing l.
func Context(ctx context.Context, l zerolog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, &l)
}

// FromContext returns the *zerolog.Logger stored in ctx. If no logger has been
// stored it falls back to the package-level base logger.
func FromContext(ctx context.Context) *zerolog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*zerolog.Logger); ok && l != nil {
		return l
	}
	return base.Load()
}

// With returns a context carrying a logger enriched with the supplied fields
// on top of whatever logger ctx already holds.
func With(ctx context.Context, fields map[string]any) context.Context {
	return Context(ctx, FromContext(ctx).With().Fields(fields).Logger())
}

// erisStackMarshaler is assigned to zerolog.ErrorStackMarshaler so that
// zerolog's .Stack() calls produce structured eris stack information.
func erisStackMarshaler(err error) any {
	if err == nil {
		return nil
	}
	return eris.ToJSON(err, true)
}
