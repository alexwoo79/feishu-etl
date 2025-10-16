package feishu

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Record 表示飞书多维表格中的一条记录
type Record struct {
	RecordID string                 `json:"record_id"`
	Fields   map[string]interface{} `json:"fields"`
}

// ListRecordsResponse 定义了获取记录列表的响应结构
type ListRecordsResponse struct {
	Code int `json:"code"`
	Data struct {
		Items     []Record `json:"items"`
		PageToken string   `json:"page_token"`
	} `json:"data"`
}

// BatchCreateRequest 批量创建记录的请求结构
type BatchCreateRequest struct {
	Records []struct {
		Fields map[string]interface{} `json:"fields"`
	} `json:"records"`
}

// BatchCreateResponse 批量创建记录的响应结构
type BatchCreateResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Records []Record `json:"records"`
	} `json:"data"`
}

// ListRecords 分页获取飞书多维表格记录
func (c *Client) ListRecords(bitableAppToken, tableID string, limit int, viewID string) ([]Record, error) {
	url := fmt.Sprintf("https://open.feishu.cn/open-apis/bitable/v1/apps/%s/tables/%s/records", bitableAppToken, tableID)
	var records []Record
	pageToken := ""

	for {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("创建获取记录请求失败: %w", err)
		}
		q := req.URL.Query()
		q.Add("page_size", strconv.Itoa(limit))
		if pageToken != "" {
			q.Add("page_token", pageToken)
		}
		if viewID != "" {
			q.Add("view_id", viewID)
		}
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Authorization", "Bearer "+c.token)

		httpResp, err := c.sendWithRetry(req, 3)
		if err != nil {
			return nil, fmt.Errorf("发送获取记录请求失败: %w", err)
		}

		body, err := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("读取记录响应失败: %w", err)
		}

		var listResp ListRecordsResponse
		if err := json.Unmarshal(body, &listResp); err != nil {
			return nil, fmt.Errorf("解析记录响应失败: %w, 响应: %s", err, string(body))
		}

		if listResp.Code != 0 {
			return nil, fmt.Errorf("获取记录失败: code=%d, 响应: %s", listResp.Code, string(body))
		}

		// 对获取到的记录进行预处理
		for i := range listResp.Data.Items {
			preprocessRecordFields(&listResp.Data.Items[i])
		}

		records = append(records, listResp.Data.Items...)

		pageToken = listResp.Data.PageToken
		if pageToken == "" {
			break
		}
	}

	return records, nil
}

// preprocessRecordFields 预处理记录字段，将复合对象转换为字符串
func preprocessRecordFields(record *Record) {
	for fieldName, fieldValue := range record.Fields {
		// 特别处理"检查"字段
		if fieldName == "检查" {
			// 提取文本内容
			textContent := extractTextFromField(fieldValue)
			// 将复合对象替换为纯文本
			record.Fields[fieldName] = textContent
		}
		// 可以在这里添加其他字段的预处理逻辑
	}
}

// extractTextFromField 从飞书API返回的字段中提取文本内容
func extractTextFromField(fieldValue interface{}) string {
	// 如果是字符串类型，直接返回
	if str, ok := fieldValue.(string); ok {
		return str
	}

	// 如果是复合对象，尝试提取文本
	if fieldMap, ok := fieldValue.(map[string]interface{}); ok {
		// 检查是否存在value字段
		if valueField, exists := fieldMap["value"]; exists {
			// value字段应该是一个数组
			if valueArray, ok := valueField.([]interface{}); ok {
				// 遍历数组中的元素
				var textBuilder strings.Builder
				for _, item := range valueArray {
					if itemMap, ok := item.(map[string]interface{}); ok {
						if text, exists := itemMap["text"]; exists {
							if textStr, ok := text.(string); ok {
								textBuilder.WriteString(textStr)
							}
						}
					}
				}
				return textBuilder.String()
			}
		}
	}

	// 兜底方案：尝试直接转换为字符串
	return fmt.Sprintf("%v", fieldValue)
}

// BatchCreateRecords 批量创建记录
func (c *Client) BatchCreateRecords(bitableAppToken, tableID string, records []map[string]interface{}) error {
	url := fmt.Sprintf("https://open.feishu.cn/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_create", bitableAppToken, tableID)

	var payload BatchCreateRequest
	for _, record := range records {
		payload.Records = append(payload.Records, struct {
			Fields map[string]interface{} `json:"fields"`
		}{Fields: record})
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := c.sendWithRetry(req, 3)
	if err != nil {
		return fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %w", err)
	}

	var createResp BatchCreateResponse
	if err := json.Unmarshal(body, &createResp); err != nil {
		return fmt.Errorf("解析响应失败: %w, 响应: %s", err, string(body))
	}

	if createResp.Code != 0 {
		return fmt.Errorf("创建记录失败: code=%d, msg=%s, 响应: %s", createResp.Code, createResp.Msg, string(body))
	}

	return nil
}
