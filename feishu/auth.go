package feishu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
)

// AuthResponse 飞书认证响应
type AuthResponse struct {
	Code              int    `json:"code"`
	Msg               string `json:"msg"`
	TenantAccessToken string `json:"tenant_access_token"`
	Expire            int    `json:"expire"`
}

// GetTenantAccessToken 获取租户访问令牌
func GetTenantAccessToken(appID, appSecret string) (string, error) {
	url := "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
	payload := map[string]string{
		"app_id":     appID,
		"app_secret": appSecret,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("序列化请求失败: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	var authResp AuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return "", fmt.Errorf("解析响应失败: %w", err)
	}

	if authResp.Code != 0 {
		return "", fmt.Errorf("获取token失败: code=%d, msg=%s", authResp.Code, authResp.Msg)
	}
	if authResp.TenantAccessToken == "" {
		return "", errors.New("返回的token为空")
	}
	log.Printf("[INFO] 成功获取Token，有效期 %d 秒", authResp.Expire)
	return authResp.TenantAccessToken, nil
}
