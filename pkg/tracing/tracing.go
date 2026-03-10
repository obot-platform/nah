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
	EnvVarName         = "NAH_TRACE_LEVEL"

	DefaultLevel = LevelOff
)

func (l Level) normalized() Level {
	switch l {
	case LevelOff, LevelBasic, LevelVerbose:
		return l
	case "":
		if envLevel, ok := LevelFromEnv(); ok {
			return envLevel
		}
		return DefaultLevel
	default:
		return DefaultLevel
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
	return ParseLevel(os.Getenv(EnvVarName))
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

type Instrumentation struct {
	tracer trace.Tracer
	level  Level
}

func NewInstrumentation(scope string, level Level) Instrumentation {
	return Instrumentation{
		tracer: otel.Tracer(scope),
		level:  level.normalized(),
	}
}

func (i Instrumentation) Level() Level {
	return i.level.normalized()
}

func (i Instrumentation) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return i.StartLevel(ctx, LevelBasic, name, opts...)
}

func (i Instrumentation) StartLevel(ctx context.Context, min Level, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !i.Level().Enabled(min) {
		return ctx, noop.Span{}
	}
	if i.tracer == nil {
		return ctx, noop.Span{}
	}
	return i.tracer.Start(ctx, name, opts...)
}
