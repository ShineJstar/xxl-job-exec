package xxl

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestCallbackRetriesAfterTimeout 验证已超时的任务仍能携带原链路重试结果回调。
func TestCallbackRetriesAfterTimeout(t *testing.T) {
	var attempts atomic.Int32
	complete := make(chan HandleCallbackParam, 1)
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/callback" {
			if !strings.Contains(r.Header.Get("traceparent"), "4bf92f3577b34da6a3ce929d0e0e4736") {
				t.Error("callback trace lost")
			}
			if attempts.Add(1) < 3 {
				io.WriteString(w, `{"code":500,"msg":"retry"}`)
				return
			}
			var batch CallbackParamList
			_ = json.NewDecoder(r.Body).Decode(&batch)
			complete <- batch[0]
		}
		io.WriteString(w, `{"code":200}`)
	}))
	defer admin.Close()
	e := NewExecutor(ServerAddr(admin.URL), ExecutorURL("http://localhost:9999"), AccessToken("test-token"))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	e.RegTask("timeout", func(ctx context.Context, _ *Task) error { <-ctx.Done(); return nil })
	result := triggerTask(t, e, TriggerParam{JobID: 1, LogID: 1, GlueType: "BEAN", ExecutorHandler: "timeout", ExecutorTimeout: 1, TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
	if result.Code != 200 {
		t.Fatal(result)
	}
	select {
	case result := <-complete:
		if result.HandleCode == 200 {
			t.Fatal("timeout reported success")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("cancelled callback not retried")
	}
	if attempts.Load() != 3 {
		t.Fatal(attempts.Load())
	}
}
