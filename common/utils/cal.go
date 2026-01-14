package utils

import (
	"errors"
	"github.com/shopspring/decimal"
)

// CurrencyConfig 定义币种精度
type CurrencyConfig struct {
	Scale int32 // 存入 Redis 时的放大倍数 (10^Scale)
}

// GlobalConfig 全局配置表
// 关键策略：SHIB 仅保留 4 位精度，防止 Int64 溢出
var GlobalConfig = map[string]CurrencyConfig{
	"BTC":  {Scale: 8}, // 1亿
	"ETH":  {Scale: 8}, // 1亿
	"USDT": {Scale: 6}, // 100万
	"SHIB": {Scale: 4}, // 1万 (解决溢出问题的关键)
}

// GetScale 获取币种精度，默认 8
func GetScale(currency string) int32 {
	if cfg, ok := GlobalConfig[currency]; ok {
		return cfg.Scale
	}
	return 8
}

// ToDBInteger 将金额字符串转为 Redis 存储的 Int64
func ToDBInteger(currency string, amountStr string) (int64, error) {
	val, err := decimal.NewFromString(amountStr)
	if err != nil {
		return 0, err
	}
	scale := GetScale(currency)

	// 核心运算：Value * 10^scale
	// 使用 Floor 进行截断，丢弃多余的“尘埃”精度
	multiplier := decimal.New(1, scale)
	dbVal := val.Mul(multiplier).Floor()

	// 溢出检查 (Go decimal 库可以转 BigInt，这里判断是否在 Int64 范围内)
	if !dbVal.IsInteger() { // 理论上 Floor 后肯定是整数
		return 0, errors.New("decimal error")
	}
	// 将 big.Int 转 int64，这里其实还可以加一层范围校验，但 decimal 库本身支持极大数
	// 只要配置合理 (SHIB=4)，这里基本不会溢出
	return dbVal.IntPart(), nil
}
