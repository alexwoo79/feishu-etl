package util

import (
	"regexp"
	"strings"
)

// GetStringField 安全地从 map 中获取字符串字段
func GetStringField(fields map[string]interface{}, key string) string {
	if val, ok := fields[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// ReplaceNewlinesWithSemicolons 将文本中的换行符替换为分号，多个连续空格替换为一个逗号
func ReplaceNewlinesWithSemicolons(text string) string {
	// 先替换换行符为分号
	result := strings.ReplaceAll(text, "\n", ";")
	
	// 使用正则表达式将多个连续空格替换为一个逗号
	spaceRegex := regexp.MustCompile(`\s{2,}`)
	result = spaceRegex.ReplaceAllString(result, ",")
	
	return result
}