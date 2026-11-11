package xxl

import (
	"context"
	"testing"
)

// TestJavaTimeoutCode 验证任务超时使用 Java 3.3.2 的 502 状态，而不是普通失败码。
func TestJavaTimeoutCode(t *testing.T) {
	e, callbacks := testExecutor(t)
	e.RegTask("timeout", func(ctx context.Context, _ *Task) error { <-ctx.Done(); return nil })
	if result := triggerTask(t, e, TriggerParam{JobID: 1, LogID: 1, ExecutorHandler: "timeout", GlueType: "BEAN", ExecutorTimeout: 1}); result.Code != 200 {
		t.Fatal(result)
	}
	if result := awaitCallback(t, callbacks); result.HandleCode != 502 {
		t.Fatalf("expected Java timeout code 502, got %d", result.HandleCode)
	}
}
