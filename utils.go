package xxl

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
)

// jsonTo 按指定 HTTP 状态输出标准 JSON。
func jsonTo(code int, resp any, w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	return marshalWrite(w, resp)
}

// jsonToNoErr 输出协议边界响应，编码失败由 HTTP 写入层处理。
func jsonToNoErr(code int, resp any, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = marshalWrite(w, resp)
}

// marshalWrite 统一使用标准 JSON 编码器输出数据。
func marshalWrite(w io.Writer, a any) error {
	enc := json.NewEncoder(w)
	return enc.Encode(a)
}

// bind 解码请求 JSON 到明确的协议类型。
func bind(r io.Reader, p any) error {
	dec := json.NewDecoder(r)
	return dec.Decode(p)
}

// TaskJsonParam 将任务参数字符串解码为业务类型。
func TaskJsonParam(task *Task, p any) error {
	r := strings.NewReader(task.Param.ExecutorParams)
	return bind(r, p)
}

// rndUrl 从已验证的非空调度地址列表选择一个地址。
func rndUrl(urls []string) string {
	index := rand.IntN(len(urls))
	return urls[index]
}
