package util

// GetStringField 安全地从 map 中获取字符串字段
func GetStringField(fields map[string]interface{}, key string) string {
	if val, ok := fields[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}
