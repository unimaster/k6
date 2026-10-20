package trace_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"go.k6.io/k6/v2/internal/features"
	"go.k6.io/k6/v2/internal/lib/testutils"
	k6trace "go.k6.io/k6/v2/internal/lib/trace"
	"go.k6.io/k6/v2/internal/lib/trace/tracetest"
)

// nativeTracing returns the feature flags enabling the k6 execution hierarchy spans.
func nativeTracing() *features.Flags {
	return &features.Flags{NativeTracing: true}
}

func TestSpanHierarchy(t *testing.T) {
	t.Parallel()

	tp, rec := tracetest.NewProvider()
	tracer := k6trace.Tracer(tp, nativeTracing())
	logger := testutils.NewLogger(t)

	runCtx, runSpan := k6trace.StartTestRun(context.Background(), tracer, logger)
	scCtx, scSpan := k6trace.StartScenario(runCtx, tracer, k6trace.ScenarioInfo{Name: "sc", Executor: "constant-vus"})
	vuCtx, vuSpan := k6trace.StartVU(scCtx, tracer, k6trace.VUInfo{ID: 2, IDGlobal: 3, Scenario: "sc"})
	_, itSpan := k6trace.StartIteration(vuCtx, tracer, k6trace.IterationInfo{
		Number: 4, VUID: 2, VUIDGlobal: 3, VUIterationInScenario: 5, Scenario: "sc",
		ScenarioIterationInInstance: 6, ScenarioIterationInTest: 7, HasScenarioIterationNumbers: true,
	})
	for _, s := range []oteltrace.Span{itSpan, vuSpan, scSpan} {
		k6trace.EndSpan(s, nil)
	}
	runSpan.End(nil)

	byName := map[string]oteltrace.SpanContext{}
	parents := map[string]oteltrace.SpanID{}
	attrs := map[string]map[string]attribute.Value{}
	for _, s := range rec.Ended() {
		byName[s.Name()] = s.SpanContext()
		parents[s.Name()] = s.Parent().SpanID()
		attrs[s.Name()] = map[string]attribute.Value{}
		for _, kv := range s.Attributes() {
			attrs[s.Name()][string(kv.Key)] = kv.Value
		}
	}
	require.Len(t, byName, 4)

	assert.False(t, rec.ByName(k6trace.SpanRun)[0].Parent().IsValid(), "the run span is a root")
	assert.Equal(t, byName[k6trace.SpanRun].SpanID(), parents[k6trace.SpanScenario])
	assert.Equal(t, byName[k6trace.SpanScenario].SpanID(), parents[k6trace.SpanVU])
	assert.Equal(t, byName[k6trace.SpanVU].SpanID(), parents[k6trace.SpanIteration])
	for name, sc := range byName {
		assert.Equal(t, byName[k6trace.SpanRun].TraceID(), sc.TraceID(), "%s must be in the run trace", name)
	}

	assert.Equal(t, "sc", attrs[k6trace.SpanScenario]["test.scenario"].AsString())
	assert.Equal(t, "constant-vus", attrs[k6trace.SpanScenario]["k6.scenario.executor"].AsString())
	assert.Equal(t, int64(2), attrs[k6trace.SpanVU]["test.vu"].AsInt64())
	assert.Equal(t, int64(2), attrs[k6trace.SpanVU]["k6.vu.id_in_instance"].AsInt64())
	assert.Equal(t, int64(3), attrs[k6trace.SpanVU]["k6.vu.id_in_test"].AsInt64())
	assert.Equal(t, int64(4), attrs[k6trace.SpanIteration]["test.iteration.number"].AsInt64())
	assert.Equal(t, int64(2), attrs[k6trace.SpanIteration]["test.vu"].AsInt64())
	assert.Equal(t, "sc", attrs[k6trace.SpanIteration]["test.scenario"].AsString())
	assert.Equal(t, int64(2), attrs[k6trace.SpanIteration]["k6.vu.id_in_instance"].AsInt64())
	assert.Equal(t, int64(3), attrs[k6trace.SpanIteration]["k6.vu.id_in_test"].AsInt64())
	assert.Equal(t, int64(5), attrs[k6trace.SpanIteration]["k6.vu.iteration_in_scenario"].AsInt64())
	assert.Equal(t, int64(6), attrs[k6trace.SpanIteration]["k6.scenario.iteration_in_instance"].AsInt64())
	assert.Equal(t, int64(7), attrs[k6trace.SpanIteration]["k6.scenario.iteration_in_test"].AsInt64())
}

func TestStartTestRunStartsNewTrace(t *testing.T) {
	t.Parallel()

	tp, _ := tracetest.NewProvider()
	logger := testutils.NewLogger(t)

	// A span already in the context is not a parent, the run is always a new trace.
	outerCtx, outer := tp.Tracer("test").Start(context.Background(), "outer")
	defer outer.End()

	_, run := k6trace.StartTestRun(outerCtx, k6trace.Tracer(tp, nativeTracing()), logger)
	defer run.End(nil)

	assert.NotEqual(t, outer.SpanContext().TraceID(), run.SpanContext().TraceID())
}

func TestTestRunEndsWhenMarkedFinished(t *testing.T) {
	t.Parallel()

	tp, rec := tracetest.NewProvider()
	_, run := k6trace.StartTestRun(context.Background(), k6trace.Tracer(tp, nativeTracing()), testutils.NewLogger(t))

	run.MarkFinished()
	finished := time.Now()
	// What follows the execution, e.g. the --linger wait, isn't part of the run span.
	time.Sleep(10 * time.Millisecond)
	run.End(errors.New("thresholds crossed"))

	runSpan := rec.ByName(k6trace.SpanRun)
	require.Len(t, runSpan, 1)
	assert.False(t, runSpan[0].EndTime().After(finished), "the run span must end when marked finished")
	assert.Equal(t, codes.Error, runSpan[0].Status().Code, "the error known after the end is still recorded")
	assert.Equal(t, "thresholds crossed", runSpan[0].Status().Description)
}

func TestTestRunEndsOnce(t *testing.T) {
	t.Parallel()

	tp, rec := tracetest.NewProvider()
	_, run := k6trace.StartTestRun(context.Background(), k6trace.Tracer(tp, nativeTracing()), testutils.NewLogger(t))

	// A hard stop and the end of the run can both end the run span, concurrently.
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { run.End(errors.New("aborted")) })
	}
	wg.Wait()
	run.End(nil)

	runSpan := rec.ByName(k6trace.SpanRun)
	require.Len(t, runSpan, 1)
	assert.Len(t, runSpan[0].Events(), 1, "the error must be recorded once")
	assert.Equal(t, codes.Error, runSpan[0].Status().Code)
}

func TestStartFunctionsWithoutTracer(t *testing.T) {
	t.Parallel()

	logger, hook := testutils.NewLoggerWithHook(t)
	ctx := context.Background()

	runCtx, run := k6trace.StartTestRun(ctx, nil, logger)
	scCtx, scSpan := k6trace.StartScenario(ctx, nil, k6trace.ScenarioInfo{Name: "sc"})
	vuCtx, vuSpan := k6trace.StartVU(ctx, nil, k6trace.VUInfo{ID: 1})
	itCtx, itSpan := k6trace.StartIteration(ctx, nil, k6trace.IterationInfo{Number: 1})

	for _, c := range []context.Context{runCtx, scCtx, vuCtx, itCtx} {
		assert.Equal(t, ctx, c, "the context must be unchanged when tracing is disabled")
	}
	for _, s := range []oteltrace.Span{scSpan, vuSpan, itSpan} {
		assert.False(t, s.IsRecording())
	}
	assert.False(t, run.SpanContext().IsValid())
	run.MarkFinished()
	run.End(errors.New("boom"))
	assert.Empty(t, hook.Drain(), "nothing must be logged when tracing is disabled")
}

func TestEndSpanRecordsError(t *testing.T) {
	t.Parallel()

	tp, rec := tracetest.NewProvider()
	tracer := k6trace.Tracer(tp, nativeTracing())
	_, okSpan := k6trace.StartScenario(context.Background(), tracer, k6trace.ScenarioInfo{Name: "ok"})
	k6trace.EndSpan(okSpan, nil)
	_, failSpan := k6trace.StartScenario(context.Background(), tracer, k6trace.ScenarioInfo{Name: "fail"})
	k6trace.EndSpan(failSpan, errors.New("boom"))

	spans := rec.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, codes.Unset, spans[0].Status().Code)
	assert.Equal(t, codes.Error, spans[1].Status().Code)
	assert.Equal(t, "boom", spans[1].Status().Description)
	require.Len(t, spans[1].Events(), 1)
	assert.Equal(t, "exception", spans[1].Events()[0].Name)
}

func TestTracerIsNilWhenTracingIsDisabled(t *testing.T) {
	t.Parallel()

	var nilProvider *k6trace.TracerProvider
	assert.Nil(t, k6trace.Tracer(nil, nativeTracing()))
	assert.Nil(t, k6trace.Tracer(nilProvider, nativeTracing()))
	assert.Nil(t, k6trace.Tracer(k6trace.NewNoopTracerProvider(), nativeTracing()))

	tp, _ := tracetest.NewProvider()
	assert.NotNil(t, k6trace.Tracer(tp, nativeTracing()))
}

func TestTracerIsGatedByTheFeatureFlag(t *testing.T) {
	t.Parallel()

	tp, _ := tracetest.NewProvider()
	assert.Nil(t, k6trace.Tracer(tp, nil))
	assert.Nil(t, k6trace.Tracer(tp, &features.Flags{}))
	assert.NotNil(t, k6trace.Tracer(tp, &features.Flags{NativeTracing: true}))
}

func TestEndIterationDoesNotRecordInterruptionsAsErrors(t *testing.T) {
	t.Parallel()

	tp, rec := tracetest.NewProvider()
	tracer := k6trace.Tracer(tp, nativeTracing())
	_, failed := k6trace.StartIteration(context.Background(), tracer, k6trace.IterationInfo{Number: 0})
	k6trace.EndIteration(failed, errors.New("boom"), false)
	_, interrupted := k6trace.StartIteration(context.Background(), tracer, k6trace.IterationInfo{Number: 1})
	k6trace.EndIteration(interrupted, context.Canceled, true)

	spans := rec.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	assert.Equal(t, codes.Unset, spans[1].Status().Code)
	assert.Empty(t, spans[1].Events())
	assert.Contains(t, spans[1].Attributes(), attribute.Bool("k6.iteration.interrupted", true))
}

func TestIterationWithoutScenarioIterationNumbers(t *testing.T) {
	t.Parallel()

	tp, rec := tracetest.NewProvider()
	_, span := k6trace.StartIteration(context.Background(), k6trace.Tracer(tp, nativeTracing()), k6trace.IterationInfo{Number: 1})
	span.End()

	for _, kv := range rec.Ended()[0].Attributes() {
		assert.NotContains(t, []string{
			"k6.scenario.iteration_in_instance", "k6.scenario.iteration_in_test",
		}, string(kv.Key), "the scenario counters are left out when the executor doesn't provide them")
	}
}
