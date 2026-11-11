package xxl

import (
	"context"
	"errors"
	"net/url"
)

// TriggerByHandlerRequest 对应 Fantasy 调度中心的扩展触发协议。
type TriggerByHandlerRequest struct {
	AppName         string  `json:"appName"`
	ExecutorHandler string  `json:"executorHandler"`
	ExecutorParam   *string `json:"executorParam"`
	AddressList     string  `json:"addressList,omitempty"`
}

// TriggerByHandlerResponse 返回匹配到的任务和执行器身份。
type TriggerByHandlerResponse struct {
	JobID           int64  `json:"jobId"`
	JobGroupID      int64  `json:"jobGroupId"`
	AppName         string `json:"appName"`
	ExecutorHandler string `json:"executorHandler"`
}

// Client 独立提供调度中心触发接口，不启动执行器或心跳。
type Client struct{ executor *Executor }

// NewClient 校验地址并创建使用有限超时的客户端。
func NewClient(addresses []string, token string, client Doer) (*Client, error) {
	if len(addresses) == 0 {
		return nil, errors.New("admin server addresses are required")
	}
	for _, address := range addresses {
		u, err := url.Parse(address)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, errors.New("invalid admin server address")
		}
	}
	options := []Option{ServerAddr(addresses...), AccessToken(token)}
	if client != nil {
		options = append(options, SetHttpClient(client))
	}
	return &Client{executor: NewExecutor(options...)}, nil
}

// TriggerByHandler 自动注入调用方链路，nil 参数表示不覆盖任务配置。
func (c *Client) TriggerByHandler(ctx context.Context, p TriggerByHandlerRequest) (*TriggerByHandlerResponse, error) {
	if p.AppName == "" || p.ExecutorHandler == "" {
		return nil, errors.New("appName and executorHandler are required")
	}
	var result TriggerByHandlerResponse
	if err := c.executor.postData(ctx, "/api/jobTriggerByHandler", p, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
