package etl

import (
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

	for _, r := range records {
		fields := r.Fields
		check, _ := fields["检查"].(string)
		if strings.Contains(strings.ToLower(check), "重复") || strings.Contains(strings.ToLower(check), "空数据") {
			log.Printf("[WARN] 记录 %s 标记为重复/空数据，跳过", r.RecordID)
			continue
		}

		dateStr, err := util.GetDateFieldAsString(fields, cfg.DateField)
		if err != nil {
			log.Printf("[WARN] 记录 %s %s 字段无效，跳过: %v", r.RecordID, cfg.DateField, err)
			continue
		}
		if dateStr == "" {
			log.Printf("[WARN] 记录 %s %s 字段为空，跳过", r.RecordID, cfg.DateField)
			continue
		}

		baseFields := map[string]interface{}{
			"工作状态":  util.GetStringField(fields, "工作状态"),
			cfg.DateField: dateStr,
			"部门":    util.GetStringField(fields, "部门"),
			"姓名":    util.GetStringField(fields, "姓名"),
			"工作日志":  util.GetStringField(fields, "工作日志"),
			"问题与沟通": util.GetStringField(fields, "问题与沟通"),
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

	log.Printf("[DEBUG] 数据转换后筛选出 %d 条有效记录", len(transformed))
	return transformed
}
