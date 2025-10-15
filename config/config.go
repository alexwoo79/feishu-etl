package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Config 定义配置结构
type Config struct {
	AppID           string `json:"app_id"`
	AppSecret       string `json:"app_secret"`
	BitableAppToken string `json:"bitable_app_token"`
	SourceTable     string `json:"source_table"`
	TargetTable     string `json:"target_table"`
	DateField       string `json:"date_field"`
	Mode            string `json:"mode"` // "full" or "incremental"
	Days            int    `json:"days"`
	BatchSize       int    `json:"batch_size"`
	DryRun          bool   `json:"dry_run"`
	Webhook 		string `json:"robot_webhook"` // 飞书机器人Webhook地址
	ChatID          string `json:"chat_id"`       // 飞书群聊ID
	CSVOutput       bool   `json:"csv_output"`    // 是否输出CSV文件
	CSVFileName     string `json:"csv_file_name"` // CSV文件名
}

// Load 从文件加载配置
func Load(path string) (*Config, error) {
	if path == "" {
		path = "config.json"
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开配置文件失败: %w", err)
	}
	defer file.Close()

	byteValue, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(byteValue, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w, 内容: %s", err, string(byteValue[:100]))
	}

	// 设置默认值
	if cfg.Mode == "" {
		cfg.Mode = "full"
	}
	
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}
	
	if cfg.CSVFileName == "" {
		cfg.CSVFileName = "output.csv"
	}

	return &cfg, nil
}

// Validate 验证配置合法性
func (c *Config) Validate() error {
	if c.AppID == "" || c.AppSecret == "" {
		return errors.New("缺少app_id或app_secret")
	}
	if c.BitableAppToken == "" {
		return errors.New("缺少bitable_app_token")
	}
	if c.SourceTable == "" || c.TargetTable == "" {
		return errors.New("缺少source_table或target_table")
	}
	if c.Mode != "full" && c.Mode != "incremental" {
		return errors.New("mode必须为full或incremental")
	}
	return nil
}
