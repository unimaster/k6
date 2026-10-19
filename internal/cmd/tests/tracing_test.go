package tests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"go.k6.io/k6/v2/errext/exitcodes"
	"go.k6.io/k6/v2/internal/cmd"
)

// otlpCollector is a minimal OTLP/HTTP traces receiver that keeps the received spans.
type otlpCollector struct {
	mu    sync.Mutex
	spans []*tracepb.Span
}

func newOTLPCollector(t *testing.T) (*otlpCollector, string) {
	t.Helper()

	c := &otlpCollector{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		var req coltracepb.ExportTraceServiceRequest
		if !assert.NoError(t, proto.Unmarshal(body, &req)) {
			return
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		for _, rs := range req.GetResourceSpans() {
			for _, ss := range rs.GetScopeSpans() {
				c.spans = append(c.spans, ss.GetSpans()...)
			}
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	return c, srv.URL + "/v1/traces"
}

func (c *otlpCollector) byName(name string) []*tracepb.Span {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []*tracepb.Span
	for _, s := range c.spans {
		if s.GetName() == name {
			out = append(out, s)
		}
	}
	return out
}

const tracingScript = `
	export const options = {
		scenarios: {
			first: { executor: 'per-vu-iterations', vus: 2, iterations: 1 },
			second: { executor: 'shared-iterations', vus: 1, iterations: 1 },
		},
	};
	export default function () {}
`

func TestTracesOutputExportsRunHierarchy(t *testing.T) {
	t.Parallel()

	collector, endpoint := newOTLPCollector(t)
	ts := getSingleFileTestState(t, tracingScript, []string{"--traces-output=otel=" + endpoint}, 0)
	cmd.ExecuteWithGlobalState(ts.GlobalState)

	// Every span, including the run span that ends last, was flushed before k6 exited.
	runs := collector.byName("run")
	require.Len(t, runs, 1)
	run := runs[0]
	assert.Empty(t, run.GetParentSpanId(), "the run span starts a new trace")

	scenarios := collector.byName("scenario")
	vus := collector.byName("vu")
	iterations := collector.byName("iteration")
	require.Len(t, scenarios, 2)
	require.Len(t, vus, 3)
	require.Len(t, iterations, 3)

	ids := map[string]string{} // span id -> name
	ids[string(run.GetSpanId())] = "run"
	for _, group := range [][]*tracepb.Span{scenarios, vus, iterations} {
		for _, s := range group {
			assert.Equal(t, run.GetTraceId(), s.GetTraceId(), "%s must be part of the run trace", s.GetName())
			ids[string(s.GetSpanId())] = s.GetName()
		}
	}
	for _, s := range scenarios {
		assert.Equal(t, "run", ids[string(s.GetParentSpanId())])
	}
	for _, s := range vus {
		assert.Equal(t, "scenario", ids[string(s.GetParentSpanId())])
	}
	for _, s := range iterations {
		assert.Equal(t, "vu", ids[string(s.GetParentSpanId())])
	}
}

func TestTracesOutputRecordsIterationErrors(t *testing.T) {
	t.Parallel()

	collector, endpoint := newOTLPCollector(t)
	script := `export default function () { throw new Error("boom"); }`
	ts := getSingleFileTestState(t, script, []string{"--traces-output=otel=" + endpoint}, 0)
	cmd.ExecuteWithGlobalState(ts.GlobalState)

	// A failing iteration doesn't fail the test run.
	runs := collector.byName("run")
	require.Len(t, runs, 1)
	assert.Equal(t, tracepb.Status_STATUS_CODE_UNSET, runs[0].GetStatus().GetCode())

	iterations := collector.byName("iteration")
	require.Len(t, iterations, 1)
	assert.Equal(t, tracepb.Status_STATUS_CODE_ERROR, iterations[0].GetStatus().GetCode())
	assert.Contains(t, iterations[0].GetStatus().GetMessage(), "boom")
}

func TestTracesOutputRecordsRunFailure(t *testing.T) {
	t.Parallel()

	collector, endpoint := newOTLPCollector(t)
	script := `
		import exec from 'k6/execution';
		export default function () { exec.test.abort('stop'); }
	`
	ts := getSingleFileTestState(t, script, []string{"--traces-output=otel=" + endpoint}, exitcodes.ScriptAborted)
	cmd.ExecuteWithGlobalState(ts.GlobalState)

	runs := collector.byName("run")
	require.Len(t, runs, 1)
	assert.Equal(t, tracepb.Status_STATUS_CODE_ERROR, runs[0].GetStatus().GetCode())
	assert.Contains(t, runs[0].GetStatus().GetMessage(), "stop")

	// The aborted iteration is interrupted, the error belongs to the run.
	iterations := collector.byName("iteration")
	require.Len(t, iterations, 1)
	assert.Equal(t, tracepb.Status_STATUS_CODE_UNSET, iterations[0].GetStatus().GetCode())
	assert.True(t, hasBoolAttribute(iterations[0], "k6.iteration.interrupted"))
}

func TestNoTraceLogWithoutTracesOutput(t *testing.T) {
	t.Parallel()

	ts := getSingleFileTestState(t, tracingScript, nil, 0)
	cmd.ExecuteWithGlobalState(ts.GlobalState)

	for _, entry := range ts.LoggerHook.Drain() {
		assert.NotContains(t, entry.Message, "Tracing the test run")
	}
}

func TestTracesOutputLogsTraceID(t *testing.T) {
	t.Parallel()

	_, endpoint := newOTLPCollector(t)
	ts := getSingleFileTestState(t, tracingScript, []string{"--traces-output=otel=" + endpoint}, 0)
	cmd.ExecuteWithGlobalState(ts.GlobalState)

	var found bool
	for _, entry := range ts.LoggerHook.Drain() {
		if entry.Message == "Tracing the test run" {
			found = true
			assert.NotEmpty(t, entry.Data["trace_id"])
		}
	}
	assert.True(t, found, "the trace ID of the run must be logged")
}

func TestTracesOutputExportsTheRunSpanOnHardStop(t *testing.T) {
	t.Parallel()

	collector, endpoint := newOTLPCollector(t)
	script := `
		import { sleep } from 'k6';
		export const options = { iterations: 1 };
		export default function () {}
		// The first signal doesn't interrupt the teardown, the second one hard stops k6.
		export function teardown() {
			console.log('teardown started');
			sleep(60);
		}
	`
	ts := getSingleFileTestState(t, script, []string{
		"-v", "--log-output=stdout", "--traces-output=otel=" + endpoint,
	}, exitcodes.ExternalAbort)

	sendSignal := make(chan os.Signal)
	ts.SignalNotify = func(c chan<- os.Signal, signals ...os.Signal) {
		if !slices.Contains(signals, os.Interrupt) {
			return
		}
		go func() {
			for sig := range sendSignal {
				c <- sig
			}
		}()
	}
	ts.SignalStop = func(chan<- os.Signal) {}

	// k6 doesn't run its deferred functions after a hard stop, as it exits right away, so
	// the run span must have been exported by the time os.Exit is called.
	var (
		mu        sync.Mutex
		runAtExit []*tracepb.Span
		exited    bool
	)
	osExit := ts.OSExit
	ts.OSExit = func(code int) {
		mu.Lock()
		defer mu.Unlock()
		// The mock doesn't exit, so k6 later calls it again at the end of the command. Only
		// the first call, by the hard stop, would happen in a real process.
		if exited {
			return
		}
		exited = true
		runAtExit = collector.byName("run")
		osExit(code)
	}

	asyncWaitForStdoutAndRun(t, ts, 50, 100*time.Millisecond, "teardown started", func() {
		sendSignal <- syscall.SIGINT
		sendSignal <- syscall.SIGINT
		close(sendSignal)
	})
	cmd.ExecuteWithGlobalState(ts.GlobalState)

	mu.Lock()
	defer mu.Unlock()
	require.True(t, exited, "k6 must have hard stopped")
	require.Len(t, runAtExit, 1)
	assert.Equal(t, tracepb.Status_STATUS_CODE_ERROR, runAtExit[0].GetStatus().GetCode())
	assert.Contains(t, runAtExit[0].GetStatus().GetMessage(), "second")
	assert.Len(t, collector.byName("iteration"), 1, "the spans already ended are exported too")
}

func TestTracesOutputRunSpanExcludesLinger(t *testing.T) {
	t.Parallel()

	collector, endpoint := newOTLPCollector(t)
	script := `export const options = { iterations: 1 }; export default function () {}`
	ts := getSingleFileTestState(t, script, []string{
		"-v", "--log-output=stdout", "--linger", "--traces-output=otel=" + endpoint,
	}, 0)

	var lingerEnded time.Time
	sendSignal := injectMockSignalNotifier(ts)
	asyncWaitForStdoutAndRun(t, ts, 50, 100*time.Millisecond, "waiting for Ctrl+C to continue", func() {
		time.Sleep(50 * time.Millisecond)
		lingerEnded = time.Now()
		sendSignal <- syscall.SIGINT
		<-sendSignal
	})
	cmd.ExecuteWithGlobalState(ts.GlobalState)

	runs := collector.byName("run")
	require.Len(t, runs, 1)
	runEnd := time.Unix(0, int64(runs[0].GetEndTimeUnixNano()))
	assert.True(t, runEnd.Before(lingerEnded), "the run span must end before the --linger wait")
	assert.Equal(t, tracepb.Status_STATUS_CODE_UNSET, runs[0].GetStatus().GetCode(),
		"the Ctrl+C ending the --linger wait doesn't fail the run")
}

func hasBoolAttribute(span *tracepb.Span, key string) bool {
	for _, kv := range span.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetBoolValue()
		}
	}
	return false
}
