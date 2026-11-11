package xxl

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"time"
)

// TaskFunc 保留任务函数签名，业务处理必须响应 context 取消。
type TaskFunc func(context.Context, *Task) error

// taskHead 保留内部任务注册类型以兼容原有辅助文件。
type taskHead struct {
	Name string
	fn   TaskFunc
}

// Task 表示一次调度执行，不与其他 logId 共享可变状态。
type Task struct {
	ID     int64
	Name   string
	Param  TriggerParam
	ext    context.Context
	cancel context.CancelFunc
	fn     TaskFunc
	e      *Executor
}

// Context 返回携带链路、超时和取消信号的执行上下文。
func (t *Task) Context() context.Context { return t.ext }

// Logger 为日志附加链路和调度标识，并按配置写入任务文件。
func (t *Task) Logger() *slog.Logger {
	sc := trace.SpanContextFromContext(t.ext)
	attrs := []slog.Attr{slog.String("traceId", sc.TraceID().String()), slog.String("spanId", sc.SpanID().String()), slog.Int64("jobId", t.ID), slog.Int64("logId", t.Param.LogID), slog.String("handler", t.Name)}
	logger := slog.New(t.e.opts.log.Handler().WithAttrs(attrs))
	if t.e.opts.logStore != nil {
		return slog.New(&taskLogHandler{base: logger.Handler(), store: t.e.opts.logStore, param: t.Param, attrs: attrs})
	}
	return logger
}

// run 提取远端链路，异常和已取消的成功返回均转换为失败回调。
func (t *Task) run(callback func(int, string)) {
	parent := propagation.TraceContext{}.Extract(t.ext, propagation.MapCarrier{"traceparent": t.Param.TraceParent, "tracestate": t.Param.TraceState})
	ctx, span := t.e.opts.tracerProvider.Tracer("xxl-job-exec").Start(parent, t.Name, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.Int64("xxl.job_id", t.ID), attribute.Int64("xxl.log_id", t.Param.LogID)))
	defer span.End()
	if t.Param.ExecutorTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(t.Param.ExecutorTimeout)*time.Second)
		defer cancel()
	}
	t.ext = ctx
	defer t.cancel()
	if t.e.opts.logStore != nil {
		t.e.opts.logStore.begin(t.Param)
		defer t.e.opts.logStore.finish(t.Param)
	}
	started := time.Now()
	var failure error
	// 在单次执行边界恢复异常，确保一次执行只回调一次。
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				failure = fmt.Errorf("task panic: %v", recovered)
			}
		}()
		if err := ctx.Err(); err != nil {
			failure = err
			return
		}
		t.Logger().InfoContext(ctx, "task started")
		failure = t.fn(ctx, t)
		if failure == nil {
			failure = ctx.Err()
		}
	}()
	code := SuccessCode
	msg := fmt.Sprintf("jobId=%d logId=%d handler=%s traceId=%s duration=%s", t.ID, t.Param.LogID, t.Name, span.SpanContext().TraceID(), time.Since(started))
	if failure != nil {
		code = FailureCode
		if ctx.Err() == context.DeadlineExceeded {
			code = TimeoutCode
		}
		msg += " error=" + failure.Error()
		span.RecordError(failure)
		span.SetStatus(codes.Error, failure.Error())
		t.Logger().ErrorContext(ctx, "task failed", "error", failure)
	} else {
		t.Logger().InfoContext(ctx, "task completed")
	}
	callback(code, msg)
}
