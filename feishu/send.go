package feishu

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// NotificationResult 用于记录ETL执行结果的结构体
type NotificationResult struct {
	Success   bool
	Mode      string
	Duration  time.Duration
	StartTime time.Time
	Message   string
	Details   string // 详细执行信息，如处理的记录数等
}

// 飞书消息卡片结构
type cardMessage struct {
	MsgType string `json:"msg_type"`
	Card    struct {
		Config struct {
			WideScreenMode bool `json:"wide_screen_mode"`
		} `json:"config"`
		Header struct {
			Title struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
				Color   string `json:"color,omitempty"`
			} `json:"title"`
		} `json:"header"`
		Elements []interface{} `json:"elements"`
	} `json:"card"`
}

// SendNotification 通过飞书机器人发送执行结果通知
func SendNotification(webhook string, result NotificationResult) error {
	if webhook == "" {
		return nil // 未配置Webhook时不发送通知，也不报错
	}

	// 构建消息卡片
	card := buildNotificationCard(result)
	
	// 序列化消息
	payload, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	// 发送HTTP请求
	resp, err := http.Post(webhook, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("发送通知失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("飞书机器人返回非成功状态: %d", resp.StatusCode)
	}

	return nil
}

// 构建飞书消息卡片内容
func buildNotificationCard(result NotificationResult) cardMessage {
	var (
		statusIcon  string
		statusColor string
		statusText  string
		modeText string
	)
	switch result.Mode {
    case "full":
        modeText = "全量模式"
    case "incremental":
        modeText = "增量模式"
    default:
        modeText = result.Mode
    }

	// 根据执行结果设置状态样式
	if result.Success {
		statusIcon = "✅"
		statusColor = "green"
		statusText = "执行成功"
	} else {
		statusIcon = "❌"
		statusColor = "red"
		statusText = "执行失败"
	}

	// 基础卡片结构
	card := cardMessage{
		MsgType: "interactive",
	}
	card.Card.Config.WideScreenMode = true
	
	// 卡片标题
	card.Card.Header.Title.Tag = "plain_text"
	card.Card.Header.Title.Content = fmt.Sprintf("%s CRCCREDC工时数据收集转换写入任务--%s", statusIcon, statusText)
	card.Card.Header.Title.Color = statusColor

	// 卡片内容
	card.Card.Elements = []interface{}{
		// 执行基本信息
		map[string]interface{}{
			"tag": "div",
			"text": map[string]interface{}{
				"tag":     "lark_md",
				"content": fmt.Sprintf("**执行模式**: %s\n**开始时间**: %s\n**耗时**: %.2f秒\n",
					modeText,
					result.StartTime.Format("2006-01-02 15:04:05"),
					result.Duration.Seconds()),
			},
		},
		// 空行分隔
		map[string]interface{}{"tag": "hr"},
		// 结果信息
		map[string]interface{}{
			"tag": "div",
			"text": map[string]interface{}{
				"tag":     "lark_md",
				"content": fmt.Sprintf("**结果信息**: %s", result.Message),
			},
		},
	}

	// 添加详细信息（如果有）
	if result.Details != "" {
		card.Card.Elements = append(card.Card.Elements, map[string]interface{}{
			"tag": "div",
			"text": map[string]interface{}{
				"tag":     "lark_md",
				"content": fmt.Sprintf("**详细数据**: %s", result.Details),
			},
		})
	}

	return card
}
    