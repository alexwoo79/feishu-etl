package etl

import (
	"fmt"
	"log"
	"time"

	"github.com/alexwoo79/feishu-etl/config"
	"github.com/alexwoo79/feishu-etl/feishu"
	"github.com/alexwoo79/feishu-etl/util"
)

// Load 将转换后的数据加载到目标表
func Load(client *feishu.Client, cfg *config.Config, transformedData []map[string]interface{}) error {
	// 获取目标表去重键
	targetKeys, err := getTargetKeys(client, cfg)
	if err != nil {
		return fmt.Errorf("获取目标表去重键失败: %w", err)
	}

	// 过滤重复数据
	var dataToWrite []map[string]interface{}
	for _, row := range transformedData {
		dateStr := row[cfg.DateField].(string)
		name := row["姓名"].(string)
		project := row["项目名称"].(string)
		key := fmt.Sprintf("%s|%s|%s", dateStr, name, project)

		if !targetKeys[key] {
			dataToWrite = append(dataToWrite, row)
		}
	}
	log.Printf("[INFO] 去重后，需要写入 %d 条", len(dataToWrite))

	// Dry-run模式不执行实际写入
	if cfg.DryRun {
		log.Println("[DRY-RUN] dry_run 模式开启，不会写入目标表")
		log.Printf("[DRY-RUN] 本次处理数据量: %d 条", len(dataToWrite))

		// 打印几条预览数据
		for i, row := range dataToWrite {
			if i >= 3 { // 只打印前3条
				break
			}
			prepared, err := prepareRecordForWriting(row, cfg.DateField)
			if err != nil {
				log.Printf("[DRY-RUN] 准备数据 %d 失败: %v", i+1, err)
			} else {
				log.Printf("[DRY-RUN] 准备后数据预览 %d: %+v", i+1, prepared)
			}
		}
		return nil
	}

	// 批量写入数据
	if len(dataToWrite) > 0 {
		batchSize := cfg.BatchSize
		total := len(dataToWrite)
		written := 0

		for i := 0; i < total; i += batchSize {
			end := i + batchSize
			if end > total {
				end = total
			}
			batch := dataToWrite[i:end]

			// 准备写入的记录
			var preparedBatch []map[string]interface{}
			for _, row := range batch {
				preparedRow, err := prepareRecordForWriting(row, cfg.DateField)
				if err != nil {
					log.Printf("[ERROR] 准备记录失败: %v，跳过", err)
					continue
				}
				preparedBatch = append(preparedBatch, preparedRow)
			}

			if len(preparedBatch) == 0 {
				log.Println("[INFO] 当前批次无有效记录，跳过")
				continue
			}

			// 执行写入
			if err := client.BatchCreateRecords(cfg.BitableAppToken, cfg.TargetTable, preparedBatch); err != nil {
				return fmt.Errorf("写入批次失败: %w", err)
			}

			written += len(preparedBatch)
			log.Printf("[INFO] 已写入 %d/%d 条记录", written, total)
		}

		log.Printf("[INFO] 成功写入 %d/%d 条记录", written, total)
	} else {
		log.Println("[INFO] 没有需要写入的数据")
	}

	return nil
}

// getTargetKeys 获取目标表已存在的记录键，用于去重
func getTargetKeys(client *feishu.Client, cfg *config.Config) (map[string]bool, error) {
	var records []feishu.Record
	var err error

	if cfg.Mode == "incremental" {
		// 增量模式：只获取时间窗口内的记录
		endTime := time.Now()
		startTime := endTime.AddDate(0, 0, -cfg.Days)
		startDateStr := startTime.Format("2006-01-02")
		endDateStr := endTime.Format("2006-01-02")

		log.Printf("[INFO] [增量模式] 获取目标表所有记录以进行本地日期过滤...")
		allRecords, err := client.ListRecords(cfg.BitableAppToken, cfg.TargetTable, 500, "")
		if err != nil {
			return nil, fmt.Errorf("获取目标表所有记录失败: %w", err)
		}

		// 本地过滤目标记录
		log.Printf("[INFO] [增量模式] 本地过滤目标表记录，时间窗口: %s 至 %s", startDateStr, endDateStr)
		records = util.FilterRecordsByDate(allRecords, cfg.DateField, startTime, endTime)
		log.Printf("[INFO] [增量模式] 目标表中 %d 条记录在指定时间窗口内", len(records))
	} else {
		// 全量模式：获取所有记录
		log.Println("[INFO] [全量模式] 获取目标表所有记录...")
		records, err = client.ListRecords(cfg.BitableAppToken, cfg.TargetTable, 500, "")
		if err != nil {
			return nil, fmt.Errorf("获取目标表所有记录失败: %w", err)
		}
	}

	// 生成去重键
	keys := make(map[string]bool, len(records))
	for _, r := range records {
		dateStr, err := util.GetDateFieldAsString(r.Fields, cfg.DateField)
		if err != nil {
			log.Printf("[WARN] 目标表记录 %s 的 %s 字段解析失败，跳过: %v", r.RecordID, cfg.DateField, err)
			continue
		}
		if dateStr == "" {
			log.Printf("[WARN] 目标表记录 %s 缺少 %s 字段，跳过", r.RecordID, cfg.DateField)
			continue
		}

		name := util.GetStringField(r.Fields, "姓名")
		project := util.GetStringField(r.Fields, "项目名称")

		if name == "" || project == "" {
			log.Printf("[WARN] 目标表记录 %s 缺少姓名或项目名称字段，跳过", r.RecordID)
			continue
		}

		key := fmt.Sprintf("%s|%s|%s", dateStr, name, project)
		keys[key] = true
	}

	log.Printf("[DEBUG] 获取到 %d 个目标表记录键用于去重", len(keys))
	return keys, nil
}

// prepareRecordForWriting 准备写入目标表的记录（处理日期格式等）
func prepareRecordForWriting(record map[string]interface{}, dateField string) (map[string]interface{}, error) {
	prepared := make(map[string]interface{})
	for key, value := range record {
		if key == dateField {
			if dateStr, ok := value.(string); ok {
				timestampMs, err := util.ParseDateToTimestampMs(dateStr)
				if err != nil {
					return nil, fmt.Errorf("处理日期字段 '%s' 失败: %w", dateStr, err)
				}
				prepared[key] = timestampMs
			} else {
				return nil, fmt.Errorf("日期字段 '%v' 不是字符串类型", value)
			}
		} else {
			prepared[key] = value
		}
	}
	return prepared, nil
}
