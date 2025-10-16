package etl

import (
	"fmt"
	"log"
	"time"

	"github.com/alexwoo79/feishu-etl/config"
	"github.com/alexwoo79/feishu-etl/feishu"
	"github.com/alexwoo79/feishu-etl/util"
)

// Extract 从源表抽取数据
func Extract(client *feishu.Client, cfg *config.Config) ([]feishu.Record, error) {
	log.Println("[INFO] 开始抓取源表数据...")

	// 读取源表所有记录
	records, err := client.ListRecords(cfg.BitableAppToken, cfg.SourceTable, 500, "")
	if err != nil {
		return nil, fmt.Errorf("读取源表失败: %w", err)
	}

	log.Printf("[INFO] 抓取源表 %d 条", len(records))

	// 增量模式下过滤数据
	if cfg.Mode == "incremental" {
		startTime := time.Now().AddDate(0, 0, -cfg.Days)
		endTime := time.Now()
		filtered := util.FilterRecordsByDate(records, cfg.DateField, startTime, endTime)
		log.Printf("[INFO] 增量模式: %d 条符合时间窗口 (%s 至 %s)",
			len(filtered),
			startTime.Format("2006-01-02"),
			endTime.Format("2006-01-02"))
		return filtered, nil
	}

	log.Printf("[INFO] 全量模式: %d 条", len(records))
	return records, nil
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
