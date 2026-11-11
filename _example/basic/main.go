package main

import (
	"context"
	"fmt"
	xxl "github.com/it512/xxl-job-exec"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// main 在可返回错误的启动边界外报告失败。
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run 先绑定监听再注册，关闭时等待任务和结果回调。
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	executor := xxl.NewExecutor(xxl.ServerAddr("http://127.0.0.1:8080/xxl-job-admin"), xxl.AccessToken("default_token"), xxl.RegistryKey("go-example"), xxl.ExecutorURL("http://127.0.0.1:10010/xxl-job"), xxl.SetContext(ctx))
	executor.RegTask("smoke", func(ctx context.Context, task *xxl.Task) error {
		task.Logger().InfoContext(ctx, "example task executed")
		return ctx.Err()
	})
	listener, err := net.Listen("tcp", "127.0.0.1:10010")
	if err != nil {
		return err
	}
	server := &http.Server{Handler: executor.Handle("/xxl-job"), ReadHeaderTimeout: 5 * time.Second}
	defer listener.Close()
	go server.Serve(listener)
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		_ = executor.Shutdown(shutdown)
	}()
	if err := executor.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
