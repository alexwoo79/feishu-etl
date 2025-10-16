package etl

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/alexwoo79/feishu-etl/config"
	"github.com/alexwoo79/feishu-etl/feishu"
	"github.com/alexwoo79/feishu-etl/util"
)

// Transform 转换源数据
func Transform(records []feishu.Record, cfg *config.Config) []map[string]interface{} {
	var transformed []map[string]interface{}

	// 添加计数器跟踪过滤原因
	skipDuplicateCount := 0
	skipEmptyDataCount := 0
	skipOtherCount := 0

	for _, r := range records {
		fields := r.Fields

		// 使用独立的函数检查是否应该跳过该记录
		shouldSkip, skipReason := shouldSkipRecord(r, fields)
		if shouldSkip {
			log.Printf("[DEBUG] 记录 %s %s，跳过处理", r.RecordID, skipReason)

			// 更新计数器
			switch {
			case strings.Contains(skipReason, "重复"):
				skipDuplicateCount++
			case strings.Contains(skipReason, "空数据"):
				skipEmptyDataCount++
			default:
				skipOtherCount++
			}
			continue
		}

		dateStr, err := util.GetDateFieldAsString(fields, cfg.DateField)
		if err != nil {
			log.Printf("[WARN] 记录 %s %s 字段无效，跳过: %v", r.RecordID, cfg.DateField, err)
			skipOtherCount++
			continue
		}
		if dateStr == "" {
			log.Printf("[WARN] 记录 %s %s 字段为空，跳过", r.RecordID, cfg.DateField)
			skipOtherCount++
			continue
		}

		baseFields := map[string]interface{}{
			"工作状态":        util.GetStringField(fields, "工作状态"),
			cfg.DateField: dateStr,
			"部门":          util.GetStringField(fields, "部门"),
			"姓名":          util.GetStringField(fields, "姓名"),
			"工作日志":        util.GetStringField(fields, "工作日志"),
			"问题与沟通":       util.GetStringField(fields, "问题与沟通"),
		}

		projects := []struct {
			Name string
			Hour string
		}{
			{Name: util.GetStringField(fields, "项目名称-1"), Hour: util.GetStringField(fields, "项目工时-1")},
			{Name: util.GetStringField(fields, "项目名称-2"), Hour: util.GetStringField(fields, "项目工时-2")},
			{Name: util.GetStringField(fields, "项目名称-3"), Hour: util.GetStringField(fields, "项目工时-3")},
		}

		for _, p := range projects {
			if p.Name == "" || p.Hour == "" || p.Name == "None" || p.Hour == "None" {
				continue
			}
			hourFloat, err := strconv.ParseFloat(p.Hour, 64)
			if err != nil || hourFloat == 0 {
				log.Printf("[WARN] 记录 %s 项目 %s 工时无效，跳过", r.RecordID, p.Name)
				continue
			}

			row := make(map[string]interface{})
			for k, v := range baseFields {
				row[k] = v
			}
			row["项目名称"] = p.Name
			row["工时"] = hourFloat

			transformed = append(transformed, row)
		}
	}

	// 输出过滤统计信息
	log.Printf("[DEBUG] 数据转换过滤统计 - 重复记录: %d 条, 空数据记录: %d 条, 其他原因: %d 条",
		skipDuplicateCount, skipEmptyDataCount, skipOtherCount)
	log.Printf("[DEBUG] 数据转换后筛选出 %d 条有效记录", len(transformed))
	return transformed
}

// shouldSkipRecord 检查记录是否应该被跳过
func shouldSkipRecord(record feishu.Record, fields map[string]interface{}) (bool, string) {
	// 获取检查字段的值
	checkValue, exists := fields["检查"]

	// 只有当字段存在时才进行检查
	if !exists {
		return false, ""
	}

	// 处理nil值
	if checkValue == nil {
		return false, ""
	}

	// "检查"字段现在应该是字符串类型了（经过预处理）
	check, isString := checkValue.(string)
	if !isString {
		// 兜底方案：尝试转换为字符串
		check = fmt.Sprintf("%v", checkValue)
	}

	// 只有当检查字段包含关键词时才输出调试信息

	if check != "" && containsIgnoreCase(check, "空数据") {
		log.Printf("[DEBUG] 发现标记为空数据的记录 - ID: %s, 检查字段值: '%s'", record.RecordID, check)
		return true, "标记为空数据"
	}

	if check != "" && containsIgnoreCase(check, "重复填写") {
		log.Printf("[DEBUG] 发现标记为重复填写的记录 - ID: %s, 检查字段值: '%s'", record.RecordID, check)
		return true, "标记为重复填写"
	}

	return false, ""
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

// getMapKeys 获取map的所有键
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// containsIgnoreCase 检查字符串是否包含指定子串（忽略大小写）
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
