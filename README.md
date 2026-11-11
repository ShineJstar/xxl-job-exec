# XXL-JOB Go Executor — Fantasy 适配

以 it512/xxl-job-exec 的 bf4a647375c6 为基线，适配本地 Java 调度中心 3.3.2-fantasy-trace-1。保留 MIT 许可证，module 路径仍为 github.com/it512/xxl-job-exec，Go 基线 1.25.1。

## 协议

- 支持 /beat、/idleBeat、/run、/kill、/log、注册、注销和结果回调。
- 响应改为 code/msg/data，不再使用 content；自定义 LogHandler 请相应使用 Return[T].Data。
- /run 拒绝请求返回协议对象，不返回 callback 数组；任务接收和执行完成是两种状态。
- 所有执行器入口校验 XXL-JOB-ACCESS-TOKEN。Handle 支持根路径和带前缀路径。
- 仅支持注册的 Go BEAN 任务，不实现 Java GLUE 或脚本任务。
- 保留 TaskFunc(context.Context, *Task) error、RegTask、Use 和日志/HTTP 客户端注入接口。

## 使用

```go
executor := xxl.NewExecutor(
    xxl.ServerAddr("http://127.0.0.1:8080/xxl-job-admin"),
    xxl.AccessToken("your-token"),
    xxl.RegistryKey("fantasy-prediction-job"),
    xxl.ExecutorURL("http://127.0.0.1:9999/xxl-job"),
)
executor.RegTask("job.smoke", func(ctx context.Context, task *xxl.Task) error {
    task.Logger().InfoContext(ctx, "smoke task executed")
    return ctx.Err()
})
// 宿主先监听并开始服务 executor.Handle("/xxl-job")，再调用 Start。
if err := executor.Start(); err != nil {
    return err
}
defer executor.Stop()
```

Start 现在返回配置或首次注册错误；Shutdown(ctx) 停止接受任务、停止心跳、取消/等待任务和回调，再注销。HTTP Server 由宿主负责关闭；Stop 使用 30 秒默认等待预算。失败启动后也可调用 Shutdown。

单机串行按 jobId 排队；丢弃后续会拒绝忙碌任务；覆盖之前取消旧批次和队列。logId 区分执行批次，旧回调不会移除替换后的批次。超时、取消后的 nil 返回会转为失败。

Go 任务必须响应 ctx.Done()，不能强制终止任意 goroutine。执行器不保证跨重启任务恢复。

## 链路与日志

TriggerParam 的 TraceParent/TraceState 使用小写 JSON 字段 traceparent/tracestate。SDK 从任务载荷建立远端父子关系，不绑定短暂的 /run HTTP 请求 context。

通过 SetTracerProvider 复用宿主 OpenTelemetry provider。未提供时使用无导出器的本地 provider，使新任务也拥有有效 trace。SDK 不依赖 go-zero。

Task.Logger() 附加 traceId、spanId、jobId、logId、handler。SetTaskLogStore(NewFileLogStore(...)) 可同时写文件并开启真实日志分页查询。未配置存储时日志接口返回空页，不返回演示内容。

Client.TriggerByHandler(ctx, TriggerByHandlerRequest) 对接 Fantasy 扩展 OpenAPI，自动注入 W3C Header。ExecutorParam 为 nil 表示不覆盖，指向空字符串表示显式覆盖为空。stock XXL 版本不一定提供该扩展接口。

结果回调保持任务 trace，即使任务被取消仍使用独立请求超时。回调最多三次，间隔 1、2 秒，不重复执行业务，也不提供跨重启回调恢复。

## 验证

```sh
go test -count=1 ./...
go vet ./...
CGO_ENABLED=1 go test -race ./...
```

race 需要支持的 C 编译器。_example/basic 提供绑定监听后再注册、接收退出信号并关闭的示例。

## Fork 版本引用

Fantasy 适配版本发布在 `github.com/ShineJstar/xxl-job-exec`，保留原 module 和 import 路径。消费方在自身 `go.mod` 中固定替换版本：

```go
replace github.com/it512/xxl-job-exec => github.com/ShineJstar/xxl-job-exec v0.0.1-fantasy.1
```

已发布的标签不覆盖，后续修改使用新标签。消费工程的 `go.work` 不应同时引用本地 SDK，否则仍会优先使用本地源码。
