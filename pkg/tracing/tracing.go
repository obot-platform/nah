package tracing

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Level string

const (
	LevelOff     Level = "off"
	LevelBasic   Level = "basic"
	LevelVerbose Level = "verbose"

	envVarName   = "NAH_TRACE_LEVEL"
	defaultLevel = LevelOff
)

func (l Level) normalized() Level {
	switch l {
	case LevelOff, LevelBasic, LevelVerbose:
		return l
	case "":
		if envLevel, ok := LevelFromEnv(); ok {
			return envLevel
		}
		return defaultLevel
	default:
		return defaultLevel
	}
}

func ParseLevel(value string) (Level, bool) {
	switch Level(strings.ToLower(strings.TrimSpace(value))) {
	case LevelOff:
		return LevelOff, true
	case LevelBasic:
		return LevelBasic, true
	case LevelVerbose:
		return LevelVerbose, true
	default:
		return "", false
	}
}

func LevelFromEnv() (Level, bool) {
	return ParseLevel(os.Getenv(envVarName))
}

func (l Level) Enabled(min Level) bool {
	return levelRank(l.normalized()) >= levelRank(min.normalized())
}

func levelRank(l Level) int {
	switch l {
	case LevelVerbose:
		return 2
	case LevelBasic:
		return 1
	default:
		return 0
	}
}

type Tracing struct {
	tracer trace.Tracer
	level  Level
}

func New(scope string, level Level) Tracing {
	return Tracing{
		tracer: otel.Tracer(scope),
		level:  level.normalized(),
	}
}

func (t Tracing) Level() Level {
	return t.level.normalized()
}

func (t Tracing) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return t.StartLevel(ctx, LevelBasic, name, opts...)
}

func (t Tracing) StartLevel(ctx context.Context, min Level, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.Level().Enabled(min) {
		return ctx, noop.Span{}
	}
	if t.tracer == nil {
		return ctx, noop.Span{}
	}
	return t.tracer.Start(ctx, name, opts...)
}
