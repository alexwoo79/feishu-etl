package feishu

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Client 飞书API客户端
type Client struct {
	httpClient *http.Client
	token      string
}

// NewClient 创建飞书客户端
func NewClient(token string) *Client {
	return &Client{
		token: token,
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        20,
				IdleConnTimeout:     30 * time.Second,
				MaxConnsPerHost:     5,
				TLSHandshakeTimeout: 10 * time.Second,
			},
			Timeout: 30 * time.Second,
		},
	}
}

// 带重试的HTTP请求（内部通用逻辑）
func (c *Client) sendWithRetry(req *http.Request, maxRetries int) (*http.Response, error) {
	backoff := time.Second
	for i := 0; i < maxRetries; i++ {
		resp, err := c.httpClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		if resp != nil {
			resp.Body.Close()
		}

		if i == maxRetries-1 {
			if err != nil {
				return nil, fmt.Errorf("达到最大重试次数: %w", err)
			}
			return nil, fmt.Errorf("达到最大重试次数，状态码: %d", resp.StatusCode)
		}

		time.Sleep(backoff)
		backoff *= 2
	}
	return nil, errors.New("未执行任何请求")
}
