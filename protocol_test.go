package xxl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestProtocolAuthentication 验证执行器协议不会接受缺失或错误的令牌。
func TestProtocolAuthentication(t *testing.T) {
	e := NewExecutor(AccessToken("test-token"))
	for _, token := range []string{"", "wrong"} {
		req := httptest.NewRequest(http.MethodPost, "/executor/beat", strings.NewReader("{}"))
		req.Header.Set("XXL-JOB-ACCESS-TOKEN", token)
		w := httptest.NewRecorder()
		e.Handle("/executor").ServeHTTP(w, req)
		var result map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result["code"] != float64(FailureCode) {
			t.Fatalf("unauthenticated request accepted: %s", w.Body.String())
		}
	}
}

// TestProtocolLogData 验证日志响应使用 Java 3.3.2 的 data 字段。
func TestProtocolLogData(t *testing.T) {
	e := NewExecutor(AccessToken("test-token"))
	req := httptest.NewRequest(http.MethodPost, "/executor/log", strings.NewReader(`{"logId":1,"fromLineNum":1}`))
	req.Header.Set("XXL-JOB-ACCESS-TOKEN", "test-token")
	w := httptest.NewRecorder()
	e.Handle("/executor").ServeHTTP(w, req)
	var result map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["data"]; !ok {
		t.Fatalf("missing data: %s", w.Body.String())
	}
	if _, ok := result["content"]; ok {
		t.Fatal("legacy content field returned")
	}
}

// TestTriggerTraceWire 验证标准 W3C 字段不会在任务解码时丢失。
func TestTriggerTraceWire(t *testing.T) {
	const input = `{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":"vendor=value"}`
	var p TriggerParam
	if err := json.Unmarshal([]byte(input), &p); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "traceparent") || !strings.Contains(string(b), "vendor=value") {
		t.Fatalf("trace context lost: %s", b)
	}
}

// TestRootProtocolPath 验证根路径注册不会生成双斜杠地址。
func TestRootProtocolPath(t *testing.T) {
	e := NewExecutor(AccessToken("test-token"))
	req := httptest.NewRequest(http.MethodPost, "/beat", strings.NewReader("{}"))
	req.Header.Set("XXL-JOB-ACCESS-TOKEN", "test-token")
	w := httptest.NewRecorder()
	e.Handle("/").ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("root endpoint unavailable: %d %s", w.Code, w.Body.String())
	}
}
