package util

import (
	"fmt"
	"log"
	"time"

	"github.com/alexwoo79/feishu-etl/feishu"
)

const (
	minValidTimestampSec = 946684800  // 2000-01-01 00:00:00 UTC
	maxValidTimestampSec = 2145916800 // 2038-01-19 03:14:07 UTC
	minValidTimestampMs  = minValidTimestampSec * 1000
	maxValidTimestampMs  = maxValidTimestampSec * 1000
	timestampThresholdMs = 1000000000000 // 1 trillion ms，用于判断是秒还是毫秒
)

// FilterRecordsByDate 根据日期字段过滤记录
func FilterRecordsByDate(records []feishu.Record, dateField string, startTime, endTime time.Time) []feishu.Record {
	var filtered []feishu.Record
	layout := "2006-01-02"

	for _, r := range records {
		dateValueStr, err := GetDateFieldAsString(r.Fields, dateField)
		if err != nil {
			log.Printf("[WARN] 记录 %s 的 %s 字段无法获取有效字符串表示，跳过过滤: %v", r.RecordID, dateField, err)
			continue
		}
		if dateValueStr == "" {
			log.Printf("[WARN] 记录 %s 的 %s 字段为空，跳过过滤", r.RecordID, dateField)
			continue
		}

		parsedDate, err := time.Parse(layout, dateValueStr)
		if err != nil {
			log.Printf("[WARN] 记录 %s 的 %s 字段 '%s' 无法解析，跳过过滤: %v", r.RecordID, dateField, dateValueStr, err)
			continue
		}

		if (parsedDate.After(startTime) || parsedDate.Equal(startTime)) && parsedDate.Before(endTime) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// convertTimestampToTime 将 Unix 时间戳（秒或毫秒）转换为 time.Time 对象
func convertTimestampToTime(ts float64) (time.Time, error) {
	if ts < minValidTimestampSec || ts > maxValidTimestampMs {
		return time.Time{}, fmt.Errorf("timestamp out of reasonable range: %f", ts)
	}

	if ts >= timestampThresholdMs {
		// 毫秒级时间戳
		seconds := int64(ts / 1000)
		nanos := int64((ts - float64(seconds*1000)) * 1_000_000)
		return time.Unix(seconds, nanos), nil
	} else {
		// 秒级时间戳
		return time.Unix(int64(ts), 0), nil
	}
}

// GetDateFieldAsString 安全地从 map 中获取日期字段，并尝试转换为 "YYYY-MM-DD" 格式的字符串
func GetDateFieldAsString(fields map[string]interface{}, key string) (string, error) {
	val, ok := fields[key]
	if !ok {
		return "", nil // 字段不存在，返回空字符串但不报错
	}

	switch v := val.(type) {
	case string:
		return v, nil
	case float64:
		t, err := convertTimestampToTime(v)
		if err != nil {
			return "", fmt.Errorf("failed to convert timestamp %f: %w", v, err)
		}
		return t.Format("2006-01-02"), nil
	case int64:
		t, err := convertTimestampToTime(float64(v))
		if err != nil {
			return "", fmt.Errorf("failed to convert timestamp %d: %w", v, err)
		}
		return t.Format("2006-01-02"), nil
	default:
		return "", fmt.Errorf("unsupported date field type: %T (value: %v)", v, v)
	}
}

// ParseDateToTimestampMs 将日期字符串转换为毫秒级时间戳
func ParseDateToTimestampMs(dateStr string) (int64, error) {
	if dateStr == "" {
		return 0, fmt.Errorf("empty date string")
	}

	layout := "2006-01-02"
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		// 在 Windows 系统上可能无法加载 Asia/Shanghai 时区
		// 尝试使用固定时区偏移 +08:00
		// log.Printf("[WARN] 无法加载 Asia/Shanghai 时区, 使用 UTC+8 时区: %v", err)
		loc = time.FixedZone("UTC+8", 8*60*60)
	}

	tLocal, err := time.ParseInLocation(layout, dateStr, loc)
	if err != nil {
		return 0, fmt.Errorf("failed to parse date string '%s': %w", dateStr, err)
	}

	return tLocal.UnixNano() / 1e6, nil
}
