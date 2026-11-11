package xxl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestRequestCancellationDoesNotCancelTask 验证 /run 请求结束不会提前取消后台任务。
func TestRequestCancellationDoesNotCancelTask(t *testing.T) {
	e, callbacks := testExecutor(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	e.RegTask("detached", func(ctx context.Context, _ *Task) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	body, _ := json.Marshal(TriggerParam{JobID: 1, LogID: 1, GlueType: "BEAN", ExecutorHandler: "detached"})
	requestCtx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequestWithContext(requestCtx, http.MethodPost, "/executor/run", strings.NewReader(string(body)))
	request.Header.Set("XXL-JOB-ACCESS-TOKEN", "test-token")
	recorder := httptest.NewRecorder()
	e.Handle("/executor").ServeHTTP(recorder, request)
	<-entered
	cancel()
	select {
	case <-callbacks:
		t.Fatal("task cancelled with HTTP request")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if result := awaitCallback(t, callbacks); result.HandleCode != 200 {
		t.Fatal(result)
	}
}
