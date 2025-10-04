package etl

import (
	"fmt"
	"log"
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

	// 3. 数据加载（Load）
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
