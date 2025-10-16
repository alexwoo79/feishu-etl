package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/alexwoo79/feishu-etl/config"
	"github.com/alexwoo79/feishu-etl/etl"
	"github.com/alexwoo79/feishu-etl/feishu"

	"github.com/spf13/cobra"
)

const (
	// ANSI Escape Codes for colors and styles
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
	ColorBold   = "\033[1m"
)

// printBannerWithEffect 打印带动画效果的横幅
func printBannerWithEffect(bannerLines []string, color string, duration time.Duration) {
	for _, line := range bannerLines {
		fmt.Print(color)
		for _, char := range line {
			fmt.Printf("%c", char)
			time.Sleep(duration)
		}
		fmt.Print(ColorReset)
		fmt.Println()
	}
}

// simulateProgress 模拟进度条或加载过程
func simulateProgress(description string, steps int) {
	fmt.Print(description)
	for i := 0; i <= steps; i++ {
		progressChar := "="
		if i == steps {
			progressChar = ">" // 结束箭头
		}
		fmt.Print(progressChar)
		time.Sleep(50 * time.Millisecond) // 控制进度更新速度
	}
	fmt.Println(" Done!")
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "feishu_etl",
		Short: "飞书多维表格ETL工具",
		Long:  `一个使用 Go 编写的命令行工具，用于从飞书多维表格源表抽取数据，经过转换后加载到目标表。支持全量和增量模式。`,
		Run:   run,
	}

	// 添加命令行参数
	rootCmd.PersistentFlags().StringP("config", "c", "config.json", "配置文件路径")
	rootCmd.Flags().BoolP("dry-run", "d", false, "Dry-run模式，只打印不执行写入操作")
	// 添加模式选择参数，支持full和incremental，默认full
	rootCmd.Flags().StringP("mode", "m", "full", "运行模式，支持 'full'（全量）和 'incremental'（增量），默认 'full'")

	// 添加新参数 -s 发送飞书通知，-o 输出CSV文件
	rootCmd.Flags().BoolP("send", "s", false, "发送飞书通知")
	rootCmd.Flags().StringP("output", "o", "", "输出CSV文件路径")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) {
	// 清屏与横幅展示
	fmt.Print("\033[2J\033[H")
	bannerText := []string{
		"╔════════════════════════════════════════════╗",
		"    CRCCREDC 工作日志远程处理 Tool v1.1   	",
		"╚════════════════════════════════════════════╝",
	}
	colors := []string{ColorRed, ColorGreen, ColorYellow, ColorBlue, ColorPurple, ColorCyan}
	rand.Seed(time.Now().UnixNano())
	randomColor := colors[rand.Intn(len(colors))]
	printBannerWithEffect(bannerText, randomColor+ColorBold, 1*time.Millisecond)
	time.Sleep(500 * time.Millisecond)

	fmt.Println(strings.Repeat("-", 60))

	// 加载配置
	cfgPath, _ := cmd.Flags().GetString("config")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 处理dry-run标志
	dryRunFlag, _ := cmd.Flags().GetBool("dry-run")
	if dryRunFlag {
		cfg.DryRun = true
		log.Println("[INFO] 开启Dry-run模式")
	}

	// 获取并验证运行模式
	mode, _ := cmd.Flags().GetString("mode")
	// 转换为小写以支持不区分大小写的输入
	mode = strings.ToLower(mode)
	if mode != "full" && mode != "incremental" {
		log.Fatalf("无效的模式: %s，支持的模式为 'full' 和 'incremental'", mode)
	}
	log.Printf("[INFO] 运行模式: %s", mode)
	// 将模式保存到配置中，供后续ETL流程使用
	cfg.Mode = mode

	// 处理输出CSV文件参数
	outputFile, _ := cmd.Flags().GetString("output")
	if outputFile != "" {
		cfg.CSVOutput = true
		cfg.CSVFileName = outputFile
		log.Printf("[INFO] 启用CSV文件输出: %s", outputFile)
	} else {
		// 如果没有指定 -o 参数，则不输出 CSV 文件，即使配置文件中设置了 csv_output=true
		cfg.CSVOutput = false
	}

	// 获取飞书令牌
	log.Println("[INFO] 获取飞书访问令牌...")
	token, err := feishu.GetTenantAccessToken(cfg.AppID, cfg.AppSecret)
	if err != nil {
		log.Fatalf("获取令牌失败: %v", err)
	}

	// 初始化飞书客户端
	client := feishu.NewClient(token)

	// 记录任务开始时间
	startTime := time.Now()

	// 执行ETL流程（修改为接收结果和错误）
	pipelineResult, etlErr := etl.RunPipeline(client, cfg)

	// 构建通知结果
	result := feishu.NotificationResult{
		Success:   etlErr == nil,
		Mode:      cfg.Mode,
		Duration:  time.Since(startTime),
		StartTime: startTime,
		Message: func() string {
			if etlErr == nil {
				return "数据同步完成"
			}
			return etlErr.Error()
		}(),
		// 使用从ETL流程返回的实际数据
		Details: func() string {
			if pipelineResult != nil {
				return fmt.Sprintf("抽取记录: %d 条, 转换后: %d 条.",
					pipelineResult.TotalRecords,
					pipelineResult.TransformedRecords,
				)
			}
			return "未获取到执行详情"
		}(),
	}

	// 处理发送飞书通知参数
	sendFlag, _ := cmd.Flags().GetBool("send")
	if sendFlag {
		log.Println("[INFO] 发送飞书通知...")
		// 记录配置信息用于调试
		// log.Printf("[DEBUG] Webhook: '%s', ChatID: '%s'", cfg.Webhook, cfg.ChatID)

		// 检查是否配置了通知方式
		if cfg.Webhook == "" && cfg.ChatID == "" {
			log.Println("[WARN] 未配置 Webhook 或 ChatID，无法发送通知")
		} else {
			// 发送飞书通知
			if err := feishu.SendNotification(cfg.Webhook, cfg.ChatID, client, result); err != nil {
				log.Printf("发送飞书通知失败: %v", err)
			} else {
				log.Println("[INFO] 飞书通知发送成功")
			}
		}
	} else {
		log.Println("[INFO] 未启用发送通知功能")
		// 如果没有 -s 参数，则不发送通知，即使配置了 webhook 或 chatID
	}
}
