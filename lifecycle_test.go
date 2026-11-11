package xxl

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestShutdownStopsRegistration 验证注销后无心跳，且停机后的执行器拒绝任务。
func TestShutdownStopsRegistration(t *testing.T) {
	var mu sync.Mutex
	events := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		events = append(events, r.URL.Path)
		mu.Unlock()
		io.WriteString(w, `{"code":200,"data":null}`)
	}))
	defer server.Close()
	e := NewExecutor(ServerAddr(server.URL), ExecutorURL("http://localhost:9999"))
	e.opts.heartbeatInterval = 10 * time.Millisecond
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if events[len(events)-1] != "/api/registryRemove" {
		t.Fatalf("heartbeat after unregister: %v", events)
	}
	if err := e.Start(); err == nil {
		t.Fatal("closed executor restarted")
	}
}

// TestRegistrationFailure 验证首次注册失败不能进入可执行状态。
func TestRegistrationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, `{"code":500,"msg":"rejected"}`) }))
	defer server.Close()
	e := NewExecutor(ServerAddr(server.URL), ExecutorURL("http://localhost:9999"))
	if err := e.Start(); err == nil {
		t.Fatal("initial rejection ignored")
	}
	if err := e.Stop(); err != nil {
		t.Fatal(err)
	}
}
