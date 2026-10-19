package trace

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const instrumentationName = "go.k6.io/k6"

// Span names of the k6 execution hierarchy: run -> scenario -> VU -> iteration.
const (
	SpanRun       = "k6.run"
	SpanScenario  = "scenario"
	SpanVU        = "vu"
	SpanIteration = "iteration"
)

// Tracer returns the tracer of the k6 execution hierarchy spans, or nil when tracing is
// disabled, i.e. when tp is nil or a noop provider. Callers on hot paths should get the
// tracer once and keep it, as getting it from the SDK provider takes a global lock.
//
// The functions starting the spans of the hierarchy accept a nil tracer, and then return
// their context unchanged with a noop span.
func Tracer(tp *TracerProvider) oteltrace.Tracer {
	if tp == nil || !tp.enabled {
		return nil
	}
	return tp.Tracer(instrumentationName)
}

// ScenarioInfo contains the identifiers attached to a scenario span.
type ScenarioInfo struct {
	Name     string
	Executor string
}

// VUInfo contains the identifiers attached to a VU span.
type VUInfo struct {
	ID       uint64
	IDGlobal uint64
	Scenario string
}

// IterationInfo contains the identifiers attached to an iteration span.
//
// The browser module creates its spans as children of the iteration span, and adds its
// traces metadata to it.
type IterationInfo struct {
	Number                      int64
	VUID                        uint64
	VUIDGlobal                  uint64
	VUIterationInScenario       uint64
	Scenario                    string
	ScenarioIterationInInstance uint64
	ScenarioIterationInTest     uint64
	// HasScenarioIterationNumbers is false when the executor doesn't provide the
	// scenario iteration counters, which are then left out.
	HasScenarioIterationNumbers bool
}

// TestRun is the span of a test run. It can be ended from different goroutines, e.g. at the
// end of the run or on a hard stop, only the first End is recorded.
type TestRun struct {
	span    oteltrace.Span
	endOnce sync.Once

	mu         sync.Mutex
	finishedAt time.Time
}

// StartTestRun starts the span of a test run, the root of a new trace.
func StartTestRun(
	ctx context.Context, tracer oteltrace.Tracer, logger logrus.FieldLogger,
) (context.Context, *TestRun) {
	if tracer == nil {
		return ctx, &TestRun{span: noop.Span{}}
	}
	ctx, span := tracer.Start(ctx, SpanRun, oteltrace.WithNewRoot())
	logger.Info("Tracing the test run")
	return ctx, &TestRun{span: span}
}

// SpanContext returns the span context of the run span.
func (r *TestRun) SpanContext() oteltrace.SpanContext {
	return r.span.SpanContext()
}

// MarkFinished records that the test execution, including the teardown, has finished. The
// run span ends at that time, so what follows, e.g. the end-of-test summary or the --linger
// wait, isn't part of it, while its status can still reflect the final result of the test,
// e.g. crossed thresholds, which is only known later.
func (r *TestRun) MarkFinished() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finishedAt.IsZero() {
		r.finishedAt = time.Now()
	}
}

// End records err on the run span, when present, and ends it at the time MarkFinished was
// called, or now if it wasn't.
func (r *TestRun) End(err error) {
	r.endOnce.Do(func() {
		r.mu.Lock()
		finishedAt := r.finishedAt
		r.mu.Unlock()

		var opts []oteltrace.SpanEndOption
		if !finishedAt.IsZero() {
			opts = append(opts, oteltrace.WithTimestamp(finishedAt))
		}
		EndSpan(r.span, err, opts...)
	})
}

// StartScenario starts a span for a scenario, child of the span in ctx (the run span).
func StartScenario(
	ctx context.Context, tracer oteltrace.Tracer, info ScenarioInfo,
) (context.Context, oteltrace.Span) {
	if tracer == nil {
		return ctx, noop.Span{}
	}
	return tracer.Start(ctx, SpanScenario, oteltrace.WithAttributes(
		attribute.String("test.scenario", info.Name),
		attribute.String("k6.scenario.executor", info.Executor),
	))
}

// StartVU starts a span for a VU activation, child of the span in ctx (the scenario span).
func StartVU(ctx context.Context, tracer oteltrace.Tracer, info VUInfo) (context.Context, oteltrace.Span) {
	if tracer == nil {
		return ctx, noop.Span{}
	}
	return tracer.Start(ctx, SpanVU, oteltrace.WithAttributes(
		uint64Attribute("test.vu", info.ID),
		uint64Attribute("k6.vu.id_in_instance", info.ID),
		uint64Attribute("k6.vu.id_in_test", info.IDGlobal),
		attribute.String("test.scenario", info.Scenario),
	))
}

// StartIteration starts a span for an iteration, child of the span in ctx (the VU span).
func StartIteration(
	ctx context.Context, tracer oteltrace.Tracer, info IterationInfo,
) (context.Context, oteltrace.Span) {
	if tracer == nil {
		return ctx, noop.Span{}
	}
	attrs := []attribute.KeyValue{
		attribute.Int64("test.iteration.number", info.Number),
		uint64Attribute("test.vu", info.VUID),
		attribute.String("test.scenario", info.Scenario),
		uint64Attribute("k6.vu.id_in_instance", info.VUID),
		uint64Attribute("k6.vu.id_in_test", info.VUIDGlobal),
		uint64Attribute("k6.vu.iteration_in_scenario", info.VUIterationInScenario),
	}
	if info.HasScenarioIterationNumbers {
		attrs = append(attrs,
			uint64Attribute("k6.scenario.iteration_in_instance", info.ScenarioIterationInInstance),
			uint64Attribute("k6.scenario.iteration_in_test", info.ScenarioIterationInTest),
		)
	}
	return tracer.Start(ctx, SpanIteration, oteltrace.WithAttributes(attrs...))
}

// EndIteration ends an iteration span. An interrupted iteration, e.g. by the end of its
// scenario or by a test abort, is flagged as such instead of being recorded as an error, as
// it is expected for iterations to be interrupted at the end of a test.
func EndIteration(span oteltrace.Span, err error, interrupted bool) {
	if interrupted {
		span.SetAttributes(attribute.Bool("k6.iteration.interrupted", true))
		span.End()
		return
	}
	EndSpan(span, err)
}

// EndSpan records err on the span, when present, and ends it.
func EndSpan(span oteltrace.Span, err error, opts ...oteltrace.SpanEndOption) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End(opts...)
}

func uint64Attribute(key string, value uint64) attribute.KeyValue {
	return attribute.Int64(key, int64(value)) //nolint:gosec
}
