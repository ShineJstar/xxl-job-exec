package xxl

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Doer 允许调用方注入带超时、链路或测试能力的 HTTP 客户端。
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// jobState 隔离同一任务的串行队列，覆盖之前的批次不能删除新批次。
type jobState struct {
	pending []*Task
	active  *Task
}

// Executor 管理注册、任务队列、回调和可等待的关闭过程。
type Executor struct {
	opts          Options
	mu            sync.Mutex
	handlers      map[string]TaskFunc
	jobs          map[int64]*jobState
	middlewares   []Middleware
	started       bool
	stopping      bool
	root          context.Context
	cancel        context.CancelFunc
	heartbeatCtx  context.Context
	stopHeartbeat context.CancelFunc
	workers       sync.WaitGroup
	heartbeat     sync.WaitGroup
	shutdownOnce  sync.Once
	shutdownDone  chan struct{}
	shutdownErr   error
}

// NewExecutor 构造执行器，未提供追踪器时使用无导出器的本地追踪器。
func NewExecutor(options ...Option) *Executor {
	o := Options{RegistryKey: DefaultRegistryKey, AccessToken: DefaultAccessToken, log: slog.Default(), rootCtx: context.Background(), logHandler: defaultLogHandler, client: &http.Client{Timeout: 5 * time.Second}, requestTimeout: 5 * time.Second, heartbeatInterval: 20 * time.Second}
	for _, option := range options {
		option(&o)
	}
	if o.tracerProvider == nil {
		o.tracerProvider = sdktrace.NewTracerProvider()
	}
	root, cancel := context.WithCancel(o.rootCtx)
	heartbeat, stop := context.WithCancel(o.rootCtx)
	return &Executor{opts: o, handlers: make(map[string]TaskFunc), jobs: make(map[int64]*jobState), root: root, cancel: cancel, heartbeatCtx: heartbeat, stopHeartbeat: stop, shutdownDone: make(chan struct{})}
}

// Start 校验配置并完成首次注册，成功后开始接受任务和发送心跳。
func (e *Executor) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopping {
		return errors.New("executor is shutting down")
	}
	if e.started {
		return nil
	}
	if len(e.opts.ServerAddr) == 0 {
		return errors.New("admin server addresses are required")
	}
	for _, address := range append(append([]string{}, e.opts.ServerAddr...), e.opts.ExecutorURL) {
		u, err := url.Parse(address)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("invalid HTTP address: %s", address)
		}
	}
	if e.opts.RegistryKey == "" {
		return errors.New("registry key is required")
	}
	if err := e.post(e.heartbeatCtx, "/api/registry", e.registration()); err != nil {
		return fmt.Errorf("initial registration: %w", err)
	}
	e.started = true
	e.heartbeat.Add(1)
	go func() {
		defer e.heartbeat.Done()
		ticker := time.NewTicker(e.opts.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := e.post(e.heartbeatCtx, "/api/registry", e.registration()); err != nil {
					e.opts.log.Error("executor heartbeat failed", "error", err)
				}
			}
		}
	}()
	return nil
}

// Use 配置任务中间件；任务注册时按顺序包装处理函数。
func (e *Executor) Use(middlewares ...Middleware) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.middlewares = append([]Middleware{}, middlewares...)
}

// RegTask 注册函数，不复用正在执行的任务实例。
func (e *Executor) RegTask(name string, fn TaskFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if name == "" || fn == nil {
		panic("task name and handler are required")
	}
	e.handlers[name] = e.chain(fn)
}

// Stop 使用默认超时停止执行器，HTTP 监听由宿主应用关闭。
func (e *Executor) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return e.Shutdown(ctx)
}

// Shutdown 停止心跳、取消任务、等待结果回调，最后注销。
func (e *Executor) Shutdown(ctx context.Context) error {
	e.shutdownOnce.Do(func() {
		e.mu.Lock()
		e.stopping = true
		started := e.started
		e.mu.Unlock()
		e.stopHeartbeat()
		e.cancel()
		go func() {
			// 先等待心跳退出，防止注销之后又发送一次注册。
			e.heartbeat.Wait()
			e.workers.Wait()
			if started {
				e.shutdownErr = e.post(context.Background(), "/api/registryRemove", e.registration())
			}
			close(e.shutdownDone)
		}()
	})
	select {
	case <-e.shutdownDone:
		return e.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// registration 构造当前实例的执行器注册地址。
func (e *Executor) registration() RegistryParam {
	return RegistryParam{RegistryGroup: "EXECUTOR", RegistryKey: e.opts.RegistryKey, RegistryValue: strings.TrimRight(e.opts.ExecutorURL, "/")}
}

// runTask 原子提交一次任务；拒绝时返回协议对象，不返回回调数组。
func (e *Executor) runTask(w http.ResponseWriter, r *http.Request) {
	var p TriggerParam
	if err := bind(r.Body, &p); err != nil {
		jsonToNoErr(http.StatusOK, Failure("invalid task parameters"), w)
		return
	}
	if p.GlueType != "BEAN" || p.JobID <= 0 || p.LogID <= 0 {
		jsonToNoErr(http.StatusOK, Failure("only BEAN tasks with positive jobId and logId are supported"), w)
		return
	}
	e.mu.Lock()
	if !e.started || e.stopping {
		e.mu.Unlock()
		jsonToNoErr(http.StatusOK, Failure("executor is not accepting tasks"), w)
		return
	}
	fn, ok := e.handlers[p.ExecutorHandler]
	if !ok {
		e.mu.Unlock()
		jsonToNoErr(http.StatusOK, Failure("task handler not found"), w)
		return
	}
	state := e.jobs[p.JobID]
	// 活跃和排队任务共同参与重复调度检测。
	if state != nil {
		if state.active != nil && state.active.Param.LogID == p.LogID {
			e.mu.Unlock()
			jsonToNoErr(http.StatusOK, Failure("duplicate logId"), w)
			return
		}
		for _, queued := range state.pending {
			if queued.Param.LogID == p.LogID {
				e.mu.Unlock()
				jsonToNoErr(http.StatusOK, Failure("duplicate logId"), w)
				return
			}
		}
		switch p.ExecutorBlockStrategy {
		case discardLater:
			e.mu.Unlock()
			jsonToNoErr(http.StatusOK, Failure("job is running or queued"), w)
			return
		case coverEarly:
			if state.active != nil {
				state.active.cancel()
			}
			for _, task := range state.pending {
				task.cancel()
			}
			state = nil
		}
	}
	taskCtx, cancel := context.WithCancel(e.root)
	task := &Task{ID: p.JobID, Name: p.ExecutorHandler, Param: p, ext: taskCtx, cancel: cancel, fn: fn, e: e}
	newState := state == nil
	if newState {
		state = &jobState{}
		e.jobs[p.JobID] = state
	}
	// 排队阶段即标记日志活跃，避免控制台误判尚未执行的日志已经结束。
	if e.opts.logStore != nil {
		e.opts.logStore.begin(task.Param)
	}
	state.pending = append(state.pending, task)
	if newState {
		e.workers.Add(1)
		go e.runQueue(p.JobID, state)
	}
	e.mu.Unlock()
	jsonToNoErr(http.StatusOK, ReturnSuccess, w)
}

// runQueue 串行执行一个任务批次，完成时只删除自己拥有的状态。
func (e *Executor) runQueue(jobID int64, state *jobState) {
	defer e.workers.Done()
	for {
		e.mu.Lock()
		if len(state.pending) == 0 {
			if e.jobs[jobID] == state {
				delete(e.jobs, jobID)
			}
			state.active = nil
			e.mu.Unlock()
			return
		}
		task := state.pending[0]
		state.pending = state.pending[1:]
		state.active = task
		e.mu.Unlock()
		task.run(func(code int, msg string) { e.callback(task, code, msg) })
	}
}

// killTask 取消当前批次及其队列，重复取消仍返回成功。
func (e *Executor) killTask(w http.ResponseWriter, r *http.Request) {
	var p KillParam
	if err := bind(r.Body, &p); err != nil {
		jsonToNoErr(http.StatusOK, Failure("invalid kill parameters"), w)
		return
	}
	e.mu.Lock()
	if state := e.jobs[p.JobID]; state != nil {
		if state.active != nil {
			state.active.cancel()
		}
		for _, task := range state.pending {
			task.cancel()
		}
		delete(e.jobs, p.JobID)
	}
	e.mu.Unlock()
	jsonToNoErr(http.StatusOK, ReturnSuccess, w)
}

// taskLog 将查询委托给调用方配置的日志存储。
func (e *Executor) taskLog(w http.ResponseWriter, r *http.Request) {
	var p LogParam
	if err := bind(r.Body, &p); err != nil {
		jsonToNoErr(http.StatusOK, reqErrLogHandler(err), w)
		return
	}
	jsonToNoErr(http.StatusOK, e.opts.logHandler(p), w)
}

// beat 返回协议心跳，独立于业务 JWT。
func (e *Executor) beat(w http.ResponseWriter, _ *http.Request) {
	jsonToNoErr(http.StatusOK, ReturnSuccess, w)
}

// idleBeat 在存在运行或排队任务时报告忙碌。
func (e *Executor) idleBeat(w http.ResponseWriter, r *http.Request) {
	var p IdleBeatParam
	if err := bind(r.Body, &p); err != nil {
		jsonToNoErr(http.StatusOK, Failure("invalid idleBeat parameters"), w)
		return
	}
	e.mu.Lock()
	_, busy := e.jobs[p.JobID]
	e.mu.Unlock()
	if busy {
		jsonToNoErr(http.StatusOK, Failure("job is running or queued"), w)
		return
	}
	jsonToNoErr(http.StatusOK, ReturnSuccess, w)
}

// callback 只重试发送结果，使用脱离任务取消但有独立超时的上下文。
func (e *Executor) callback(task *Task, code int, msg string) {
	payload := CallbackParamList{newCallback(task.Param, code, msg)}
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		if err := e.post(context.WithoutCancel(task.ext), "/api/callback", payload); err == nil {
			return
		} else {
			task.Logger().ErrorContext(task.ext, "task callback failed", "attempt", attempt+1, "error", err)
		}
	}
}

// post 创建有超时的控制请求，并注入 W3C 链路头。
func (e *Executor) post(ctx context.Context, action string, payload any) error {
	return e.postData(ctx, action, payload, nil)
}

// postData 校验控制响应并按需要解码 Java data 字段。
func (e *Executor) postData(ctx context.Context, action string, payload any, target any) error {
	ctx, cancel := context.WithTimeout(ctx, e.opts.requestTimeout)
	defer cancel()
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	address := strings.TrimRight(rndUrl(e.opts.ServerAddr), "/") + action
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, address, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("XXL-JOB-ACCESS-TOKEN", e.opts.AccessToken)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(request.Header))
	response, err := e.opts.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("admin HTTP status %d", response.StatusCode)
	}
	var result Return[json.RawMessage]
	if err := json.NewDecoder(io.LimitReader(response.Body, 5<<20)).Decode(&result); err != nil {
		return err
	}
	if result.Code != SuccessCode {
		return fmt.Errorf("admin rejected request: code=%d msg=%s", result.Code, result.Msg)
	}
	if target != nil {
		if len(result.Data) == 0 || string(result.Data) == "null" {
			return errors.New("admin response data is missing")
		}
		return json.Unmarshal(result.Data, target)
	}
	return nil
}

// Handle 挂载根路径或指定前缀，统一校验请求方法和协议令牌。
func (e *Executor) Handle(path string) http.Handler {
	if path != "" && !strings.HasPrefix(path, "/") {
		panic("path must start with /")
	}
	path = strings.TrimRight(path, "/")
	handlers := map[string]http.HandlerFunc{"/beat": e.beat, "/idleBeat": e.idleBeat, "/run": e.runTask, "/kill": e.killTask, "/log": e.taskLog}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e.opts.AccessToken != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("XXL-JOB-ACCESS-TOKEN")), []byte(e.opts.AccessToken)) != 1 {
			jsonToNoErr(http.StatusOK, Failure("The access token is wrong."), w)
			return
		}
		handler, ok := handlers[strings.TrimPrefix(r.URL.Path, path)]
		if !ok || !strings.HasPrefix(r.URL.Path, path) {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			jsonToNoErr(http.StatusOK, Failure("HTTP method not supported"), w)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 5<<20)
		defer r.Body.Close()
		handler(w, r)
	})
}
