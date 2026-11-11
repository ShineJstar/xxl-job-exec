package xxl

import (
	"context"
	"encoding/json"
	"errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// testExecutor 启动真实 HTTP 协议对端并收集实际回调。
func testExecutor(t *testing.T) (*Executor, chan HandleCallbackParam) {
	t.Helper()
	callbacks := make(chan HandleCallbackParam, 100)
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("XXL-JOB-ACCESS-TOKEN") != "test-token" {
			t.Error("missing outbound authentication")
		}
		if r.URL.Path == "/xxl-job-admin/api/callback" {
			var batch CallbackParamList
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Error(err)
			}
			for _, v := range batch {
				callbacks <- v
			}
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"code":200,"msg":"success","data":null}`)
	}))
	t.Cleanup(admin.Close)
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	e := NewExecutor(ServerAddr(admin.URL+"/xxl-job-admin/"), AccessToken("test-token"), ExecutorURL("http://127.0.0.1:9999/executor"), SetTracerProvider(provider), SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return e, callbacks
}

// triggerTask 通过执行器 HTTP 边界提交任务，不绕过认证和协议解析。
func triggerTask(t *testing.T, e *Executor, p TriggerParam) Return[string] {
	t.Helper()
	b, _ := json.Marshal(p)
	req := httptest.NewRequest(http.MethodPost, "/executor/run", strings.NewReader(string(b)))
	req.Header.Set("XXL-JOB-ACCESS-TOKEN", "test-token")
	w := httptest.NewRecorder()
	e.Handle("/executor").ServeHTTP(w, req)
	var result Return[string]
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("%v: %s", err, w.Body.String())
	}
	return result
}

// awaitCallback 限时等待实际任务结果，防止错误实现挂住测试。
func awaitCallback(t *testing.T, c <-chan HandleCallbackParam) HandleCallbackParam {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("callback timeout")
		return HandleCallbackParam{}
	}
}

// TestTaskTraceAndCallback 验证任务、下游触发和结果回调使用同一个 trace。
func TestTaskTraceAndCallback(t *testing.T) {
	e, callbacks := testExecutor(t)
	seen := make(chan trace.SpanContext, 1)
	e.RegTask("trace", func(ctx context.Context, task *Task) error { seen <- trace.SpanContextFromContext(ctx); return nil })
	p := TriggerParam{JobID: 1, LogID: 11, ExecutorHandler: "trace", GlueType: "BEAN", TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", TraceState: "vendor=value"}
	if r := triggerTask(t, e, p); r.Code != 200 {
		t.Fatalf("%+v", r)
	}
	sc := <-seen
	if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || sc.SpanID().String() == "00f067aa0ba902b7" || sc.TraceState().String() != "vendor=value" {
		t.Fatalf("invalid child span: %v", sc)
	}
	if v := awaitCallback(t, callbacks); v.HandleCode != 200 || v.LogID != 11 {
		t.Fatalf("%+v", v)
	}
}

// TestSerialQueue 验证单机串行不会把排队任务作为冲突丢弃。
func TestSerialQueue(t *testing.T) {
	e, callbacks := testExecutor(t)
	entered := make(chan int64, 3)
	release := make(chan struct{})
	e.RegTask("serial", func(ctx context.Context, task *Task) error {
		entered <- task.Param.LogID
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	for _, id := range []int64{1, 2} {
		if r := triggerTask(t, e, TriggerParam{JobID: 7, LogID: id, ExecutorHandler: "serial", GlueType: "BEAN", ExecutorBlockStrategy: "SERIAL_EXECUTION"}); r.Code != 200 {
			t.Fatalf("%+v", r)
		}
	}
	if id := <-entered; id != 1 {
		t.Fatalf("wrong first task: %d", id)
	}
	select {
	case <-entered:
		t.Fatal("serial tasks overlap")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if id := <-entered; id != 2 {
		t.Fatalf("wrong queued task: %d", id)
	}
	for i := 0; i < 2; i++ {
		if v := awaitCallback(t, callbacks); v.HandleCode != 200 {
			t.Fatalf("%+v", v)
		}
	}
}

// TestCoverAndDiscard 验证覆盖后的新任务不会被旧任务完成回调误删。
func TestCoverAndDiscard(t *testing.T) {
	e, callbacks := testExecutor(t)
	entered := make(chan int64, 4)
	release := make(chan struct{})
	e.RegTask("blocking", func(ctx context.Context, task *Task) error {
		entered <- task.Param.LogID
		if task.Param.LogID == 1 {
			<-ctx.Done()
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	p := TriggerParam{JobID: 9, LogID: 1, ExecutorHandler: "blocking", GlueType: "BEAN"}
	if r := triggerTask(t, e, p); r.Code != 200 {
		t.Fatal(r)
	}
	<-entered
	p.LogID = 2
	p.ExecutorBlockStrategy = "COVER_EARLY"
	if r := triggerTask(t, e, p); r.Code != 200 {
		t.Fatal(r)
	}
	<-entered
	if v := awaitCallback(t, callbacks); v.HandleCode == 200 || v.LogID != 1 {
		t.Fatalf("cancelled task reported success: %+v", v)
	}
	p.LogID = 3
	p.ExecutorBlockStrategy = "DISCARD_LATER"
	if r := triggerTask(t, e, p); r.Code != 500 {
		t.Fatalf("running replacement disappeared: %+v", r)
	}
	close(release)
	if v := awaitCallback(t, callbacks); v.LogID != 2 || v.HandleCode != 200 {
		t.Fatal(v)
	}
}

// TestTaskFailures 验证异常、业务错误和取消不会被当作成功。
func TestTaskFailures(t *testing.T) {
	e, callbacks := testExecutor(t)
	e.RegTask("error", func(context.Context, *Task) error { return errors.New("business failure") })
	e.RegTask("panic", func(context.Context, *Task) error { panic("unexpected") })
	e.RegTask("timeout", func(ctx context.Context, _ *Task) error { <-ctx.Done(); return nil })
	for i, name := range []string{"error", "panic", "timeout"} {
		p := TriggerParam{JobID: int64(i + 1), LogID: int64(i + 1), ExecutorHandler: name, GlueType: "BEAN", ExecutorTimeout: 1}
		if r := triggerTask(t, e, p); r.Code != 200 {
			t.Fatal(r)
		}
		if v := awaitCallback(t, callbacks); v.HandleCode == 200 {
			t.Fatalf("%s reported success", name)
		}
	}
	if r := triggerTask(t, e, TriggerParam{JobID: 99, LogID: 99, ExecutorHandler: "missing", GlueType: "BEAN"}); r.Code != 500 {
		t.Fatal(r)
	}
}

// TestConcurrentTraceIsolation 验证并发的独立任务不会共享 trace 上下文。
func TestConcurrentTraceIsolation(t *testing.T) {
	e, callbacks := testExecutor(t)
	var mu sync.Mutex
	seen := make(map[string]bool)
	e.RegTask("root", func(ctx context.Context, _ *Task) error {
		sc := trace.SpanContextFromContext(ctx)
		if !sc.IsValid() {
			return errors.New("invalid span")
		}
		mu.Lock()
		defer mu.Unlock()
		seen[sc.TraceID().String()] = true
		return nil
	})
	for i := int64(1); i <= 12; i++ {
		if r := triggerTask(t, e, TriggerParam{JobID: i, LogID: i, ExecutorHandler: "root", GlueType: "BEAN", TraceParent: "invalid"}); r.Code != 200 {
			t.Fatal(r)
		}
	}
	for i := 0; i < 12; i++ {
		if v := awaitCallback(t, callbacks); v.HandleCode != 200 {
			t.Fatal(v)
		}
	}
	if len(seen) != 12 {
		t.Fatalf("trace contexts reused: %d", len(seen))
	}
}
