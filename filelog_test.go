package xxl

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestTaskFileLog 验证任务日志可分页读取，包含业务日志和 trace 标识。
func TestTaskFileLog(t *testing.T) {
	e, callbacks := testExecutor(t)
	store, err := NewFileLogStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e.opts.logStore = store
	e.opts.logHandler = store.Handler
	e.RegTask("logged", func(ctx context.Context, task *Task) error {
		task.Logger().InfoContext(ctx, "business smoke")
		return nil
	})
	date := time.Now().UnixMilli()
	if r := triggerTask(t, e, TriggerParam{JobID: 1, LogID: 1, LogDateTime: date, GlueType: "BEAN", ExecutorHandler: "logged"}); r.Code != 200 {
		t.Fatal(r)
	}
	awaitCallback(t, callbacks)
	result := store.Handler(LogParam{LogID: 1, LogDateTim: date, FromLineNum: 1})
	if result.Code != 200 || !strings.Contains(result.Data.LogContent, "business smoke") || !strings.Contains(result.Data.LogContent, "traceId") {
		t.Fatalf("%+v", result)
	}
}
