package xxl

import (
	"context"
	"encoding/json"
	"go.opentelemetry.io/otel/propagation"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestTriggerClientTrace 验证触发请求保持 Header 链路和 null 参数覆盖语义。
func TestTriggerClientTrace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/api/jobTriggerByHandler" {
			t.Errorf("wrong URL: %s", r.URL)
		}
		if r.Header.Get("traceparent") != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
			t.Errorf("trace header lost: %s", r.Header.Get("traceparent"))
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["appName"] != "prediction-job" || body["executorParam"] != nil {
			t.Errorf("wrong payload: %+v", body)
		}
		io.WriteString(w, `{"code":200,"data":{"jobId":12,"jobGroupId":3,"appName":"prediction-job","executorHandler":"smoke"}}`)
	}))
	defer server.Close()
	c, err := NewClient([]string{server.URL + "/admin/"}, "token", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
	result, err := c.TriggerByHandler(ctx, TriggerByHandlerRequest{AppName: "prediction-job", ExecutorHandler: "smoke"})
	if err != nil || result.JobID != 12 {
		t.Fatalf("%+v %v", result, err)
	}
}
