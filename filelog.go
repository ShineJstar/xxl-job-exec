package xxl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileLogStore 按调度日期和 logId 存储日志，不强制任何固定时区。
type FileLogStore struct {
	root   string
	mu     sync.Mutex
	active map[string]bool
}

// NewFileLogStore 创建任务日志目录，失败时阻止应用假启动。
func NewFileLogStore(root string) (*FileLogStore, error) {
	if root == "" {
		return nil, errors.New("task log path is required")
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	return &FileLogStore{root: root, active: make(map[string]bool)}, nil
}

// filename 只使用协议中的数字标识构造路径，日期使用系统时区。
func (s *FileLogStore) filename(date, id int64) string {
	return filepath.Join(s.root, time.UnixMilli(date).Format("2006-01-02"), fmt.Sprintf("%d.log", id))
}

// begin 记录任务活跃状态，查询当前文件末尾时不会误报日志结束。
func (s *FileLogStore) begin(p TriggerParam) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[s.filename(p.LogDateTime, p.LogID)] = true
}

// finish 清除任务活跃标识，重启后旧日志默认视为已结束。
func (s *FileLogStore) finish(p TriggerParam) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, s.filename(p.LogDateTime, p.LogID))
}

// write 追加完整 JSON 行，多个任务并发写入时不交错。
func (s *FileLogStore) write(ctx context.Context, p TriggerParam, record slog.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := s.filename(p.LogDateTime, p.LogID)
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	return slog.NewJSONHandler(file, &slog.HandlerOptions{Level: slog.LevelDebug}).Handle(ctx, record)
}

// Handler 分页返回最多一千行真实日志，保留 Java 日志行号语义。
func (s *FileLogStore) Handler(p LogParam) Return[LogResult] {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.FromLineNum < 1 {
		p.FromLineNum = 1
	}
	name := s.filename(p.LogDateTim, p.LogID)
	result := LogResult{FromLineNum: p.FromLineNum, ToLineNum: p.FromLineNum - 1, IsEnd: !s.active[name]}
	file, err := os.Open(name)
	if os.IsNotExist(err) {
		return Return[LogResult]{Code: SuccessCode, Data: result}
	}
	if err != nil {
		return reqErrLogHandler(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 5<<20)
	var content strings.Builder
	line := 0
	for scanner.Scan() {
		line++
		if line < p.FromLineNum {
			continue
		}
		if line >= p.FromLineNum+1000 {
			result.IsEnd = false
			break
		}
		content.WriteString(scanner.Text())
		content.WriteByte('\n')
		result.ToLineNum = line
	}
	if err := scanner.Err(); err != nil {
		return reqErrLogHandler(err)
	}
	result.LogContent = content.String()
	return Return[LogResult]{Code: SuccessCode, Data: result}
}

// taskLogHandler 将相同任务日志同时交给宿主日志器和文件存储。
type taskLogHandler struct {
	base   slog.Handler
	store  *FileLogStore
	param  TriggerParam
	attrs  []slog.Attr
	groups []string
}

// Enabled 使用宿主日志级别，保持业务日志过滤行为一致。
func (h *taskLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

// Handle 写入带上下文的日志记录，并保留所有任务属性。
func (h *taskLogHandler) Handle(ctx context.Context, r slog.Record) error {
	// 直接复用宿主格式后的属性，同时为任务文件保留可检索的元数据。
	copy := r.Clone()
	copy.AddAttrs(h.attrs...)
	first := h.base.Handle(ctx, r)
	return errors.Join(first, h.store.write(ctx, h.param, copy))
}

// WithAttrs 返回新的包装器，避免任务之间共享可变属性切片。
func (h *taskLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copy := *h
	copy.base = h.base.WithAttrs(attrs)
	copy.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &copy
}

// WithGroup 保留结构化日志分组，不修改已有包装器。
func (h *taskLogHandler) WithGroup(name string) slog.Handler {
	copy := *h
	copy.base = h.base.WithGroup(name)
	copy.groups = append(append([]string{}, h.groups...), name)
	return &copy
}
