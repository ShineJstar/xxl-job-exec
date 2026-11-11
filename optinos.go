package xxl

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"time"
)

type Options struct {
	AccessToken string //请求令牌
	ExecutorURL string //本地(执行器) URL
	RegistryKey string //执行器名称

	ServerAddr []string //调度中心地址

	log               *slog.Logger
	client            Doer
	rootCtx           context.Context
	logHandler        LogHandler
	tracerProvider    trace.TracerProvider
	requestTimeout    time.Duration
	heartbeatInterval time.Duration
	logStore          *FileLogStore
}

type Option func(o *Options)

var (
	DefaultExecutorPort = "19999"
	DefaultRegistryKey  = "golang-jobs-plus"
	DefaultAccessToken  = "default_token"
)

// ServerAddr 设置调度中心候选地址列表。
func ServerAddr(urls ...string) Option {
	return func(o *Options) {
		o.ServerAddr = urls
	}
}

// AccessToken 设置控制请求和执行器入站请求的共享令牌。
func AccessToken(token string) Option {
	return func(o *Options) {
		o.AccessToken = token
	}
}

// ExecutorURL 设置可被调度中心访问的执行器完整地址。
func ExecutorURL(url string) Option {
	return func(o *Options) {
		o.ExecutorURL = url
	}
}

// RegistryKey 设置当前应用的执行器 AppName。
func RegistryKey(registryKey string) Option {
	return func(o *Options) {
		o.RegistryKey = registryKey
	}
}

// SetLogger 注入宿主的标准 slog 日志器。
func SetLogger(l *slog.Logger) Option {
	return func(o *Options) {
		o.log = l
	}
}

// SetContext 设置执行器生命周期上下文，不使用单次 HTTP 请求的上下文。
func SetContext(ctx context.Context) Option {
	return func(o *Options) {
		o.rootCtx = ctx
	}
}

// SetLogHandler 替换控制台日志分页查询函数。
func SetLogHandler(h LogHandler) Option {
	return func(o *Options) {
		o.logHandler = h
	}
}

// SetHttpClient 注入调用方拥有的 HTTP 客户端。
func SetHttpClient(doer Doer) Option {
	return func(o *Options) {
		o.client = doer
	}
}

// SetTracerProvider 复用宿主应用的标准 OpenTelemetry provider。
func SetTracerProvider(provider trace.TracerProvider) Option {
	return func(o *Options) { o.tracerProvider = provider }
}

// SetTaskLogStore 启用任务文件写入和 XXL 控制台日志查询。
func SetTaskLogStore(store *FileLogStore) Option {
	return func(o *Options) { o.logStore = store; o.logHandler = store.Handler }
}
