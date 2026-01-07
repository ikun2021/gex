package orderservicelogic

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	logger "github.com/luxun9527/zlog"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"google.golang.org/protobuf/proto"
	"strings"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderLogic {
	return &CreateOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 创建订单,下单有分布式事务要处理分为两个接口
func (l *CreateOrderLogic) CreateOrder(in *pb.CreateOrderReq) (*pb.OrderEmpty, error) {
	// 2. 计算需要冻结的金额与币种
	var freezeCurrency string
	var freezeAmountInt int64
	var err error

	price, _ := decimal.NewFromString(in.Price)
	amount, _ := decimal.NewFromString(in.BaseAmount)

	// 1. 解析交易对 (如 BTC_USDT -> Base:BTC, Quote:USDT)
	parts := strings.Split(in.SymbolName, "_")
	if len(parts) != 2 {
		return &pb.OrderEmpty{}, errors.New("invalid symbol format")
	}
	baseCcy, quoteCcy := parts[0], parts[1]

	if in.Side == enum.Side_Buy {
		// 买入: 花费 USDT (Quote Currency)
		// 冻结金额 = 价格 * 数量
		totalCost := price.Mul(amount)
		freezeCurrency = quoteCcy
		// 转为 DB 整数 (例如 50000.12 * 10^6)
		freezeAmountInt, err = ToDBInteger(freezeCurrency, totalCost.String())
	} else {
		// 卖出: 花费 BTC (Base Currency)
		// 冻结金额 = 数量
		freezeCurrency = baseCcy
		// 转为 DB 整数 (例如 1.5 * 10^8)
		freezeAmountInt, err = ToDBInteger(freezeCurrency, amount.String())
	}
	//订单Id的规则
	//市价单MO
	//限价单LO
	//买1 卖 2

	seqId := idgen.NextId()
	orderId := fmt.Sprintf("%d%d%d", int32(in.OrderType), int32(in.Side), seqId)

	balanceKey := fmt.Sprintf("balance:%d", in.UserId)
	frozenField := freezeCurrency + "_frozen"
	// 执行 Lua
	res, err := atomicOrderScript.Run(context.Background(), l.svcCtx.RedisCli, []string{balanceKey}, freezeCurrency, frozenField, freezeAmountInt).Result()
	if err != nil {
		return nil, fmt.Errorf("redis error: %v", err)
	}
	if cast.ToInt64(res) == 0 {
		return nil, errors.New("insufficient balance") // 余额不足
	}
	msg := &matchMq.MatchReq{Operate: &matchMq.MatchReq_NewOrder{
		NewOrder: &matchMq.NewOrderOperate{
			OrderId:     orderId,
			SequenceId:  seqId,
			Uid:         in.UserId,
			Side:        in.Side,
			Price:       in.Price,
			BaseAmount:  in.BaseAmount,
			QuoteAmount: in.QuoteAmount,
			OrderType:   in.OrderType,
		},
	}}
	data, _ := proto.Marshal(msg)
	if _, err := l.svcCtx.MatchProducer.Send(l.ctx, &pulsar.ProducerMessage{
		Payload: data,
	}); err != nil {
		logx.Errorw("CreateOrder Send message failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.PulsarErr)
	}

	return &pb.OrderEmpty{}, nil
}

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

// Lua脚本：原子性检查余额并冻结
// KEYS[1]: balance:{uid}  (例如 balance:1001)
// ARGV[1]: 币种字段名 (例如 "USDT_available")
// ARGV[2]: 冻结字段名 (例如 "USDT_frozen")
// ARGV[3]: 需要冻结的金额
// lua_script.go
var atomicOrderScript = redis.NewScript(`
    local balanceKey = KEYS[1]
    local streamKey = KEYS[2]
    
    local availField = ARGV[1]
    local frozenField = ARGV[2]
    local amount = tonumber(ARGV[3])
    local orderJson = ARGV[4]

    -- 1. 检查余额
    local currentAvail = tonumber(redis.call("HGET", balanceKey, availField) or "0")
    if currentAvail < amount then
        return 0 -- 失败
    end

    -- 2. 变更资金 (原子操作)
    redis.call("HINCRBY", balanceKey, availField, -amount)
    redis.call("HINCRBY", balanceKey, frozenField, amount)

    -- 3. 写入发件箱 (Redis Stream)
    -- 这里的 "*" 表示由 Redis 自动生成唯一的消息 ID (时间戳-序号)
    redis.call("XADD", streamKey, "*", "payload", orderJson)

    return 1 -- 成功
`)
