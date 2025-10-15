package etl

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alexwoo79/feishu-etl/config"
	"github.com/alexwoo79/feishu-etl/feishu"
)

// 定义ETL执行结果结构体，用于传递详细信息
type PipelineResult struct {
	TotalRecords    int           // 总记录数
	TransformedRecords int        // 转换后的记录数
	Duration        time.Duration // 执行耗时
}

// RunPipeline 执行完整的ETL流程，并返回执行结果
func RunPipeline(client *feishu.Client, cfg *config.Config) (*PipelineResult, error) {
	log.Println("===== [ETL START] =====")
	startTime := time.Now()

	// 1. 数据抽取（Extract）
	sourceRecords, err := Extract(client, cfg)
	if err != nil {
		return nil, fmt.Errorf("数据抽取失败: %w", err)
	}
	total := len(sourceRecords)
	log.Printf("抽取到 %d 条原始数据", total)

	// 2. 数据转换（Transform）
	transformedData := Transform(sourceRecords, cfg)
	transformedCount := len(transformedData)
	log.Printf("转换后得到 %d 条数据", transformedCount)

	// 3. 输出CSV文件（如果启用）
	if cfg.CSVOutput {
		if csvErr := outputToCSV(transformedData, cfg.CSVFileName); csvErr != nil {
			log.Printf("[WARN] 输出CSV文件失败: %v", csvErr)
		} else {
			log.Printf("[INFO] 成功输出CSV文件: %s", cfg.CSVFileName)
		}
	}

	// 4. 数据加载（Load）
	if err := Load(client, cfg, transformedData); err != nil {
		return nil, fmt.Errorf("数据加载失败: %w", err)
	}

	duration := time.Since(startTime)
	result := &PipelineResult{
		TotalRecords:       total,
		TransformedRecords: transformedCount,
		Duration:           duration,
	}
	
	log.Printf("===== [ETL DONE] 总耗时: %.2f秒=====", duration.Seconds())
	return result, nil
}

// outputToCSV 将数据输出为CSV文件
func outputToCSV(data []map[string]interface{}, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("创建CSV文件失败: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if len(data) == 0 {
		return nil
	}

	// 收集所有可能的字段名作为表头
	headers := make([]string, 0)
	headerMap := make(map[string]bool)
	
	for _, record := range data {
		for key := range record {
			if !headerMap[key] {
				headerMap[key] = true
				headers = append(headers, key)
			}
		}
	}

	// 写入表头
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("写入CSV表头失败: %w", err)
	}

	// 写入数据行
	for _, record := range data {
		row := make([]string, len(headers))
		for i, header := range headers {
			if val, ok := record[header]; ok {
				row[i] = fmt.Sprintf("%v", val)
			} else {
				row[i] = ""
			}
		}
		
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("写入CSV数据行失败: %w", err)
		}
	}

	return nil
}