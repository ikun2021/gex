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
	"github.com/ikun2021/gex/common/utils"
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
		freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, totalCost.String())
	} else {
		// 卖出: 花费 BTC (Base Currency)
		// 冻结金额 = 数量
		freezeCurrency = baseCcy
		// 转为 DB 整数 (例如 1.5 * 10^8)
		freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, amount.String())
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

// Lua脚本：原子性检查余额并冻结
// KEYS[1]: balance:{uid}  (例如 balance:1001)
// ARGV[1]: 币种字段名 (例如 "USDT_available")
// ARGV[2]: 冻结字段名 (例如 "USDT_frozen")
// ARGV[3]: 需要冻结的金额
// lua_script.go
var atomicOrderScript = redis.NewScript(`
-- Lua Script: freeze_publish_cache.lua
-- 保证 冻结资金、发送MQ、缓存订单详情 的原子性

-- KEYS 列表
local balanceKey = KEYS[1]  -- balance:{uid}
local streamKey  = KEYS[2]  -- order_events (MQ)
local orderKey   = KEYS[3]  -- order:{order_id} (详情)

-- ARGV 参数列表
local availField   = ARGV[1] -- "USDT"
local frozenField  = ARGV[2] -- "USDT_frozen"
local changeAmount = tonumber(ARGV[3]) -- 变动金额(整数)
local orderData    = ARGV[4] -- 紧凑JSON字符串
local orderTTL     = ARGV[5] -- 订单在Redis缓存的时间(秒)，如 600

-- 1. 检查余额 (Read)
local currentAvail = tonumber(redis.call("HGET", balanceKey, availField) or "0")
if currentAvail < changeAmount then
    return 0 -- 失败: 余额不足
end

-- 2. 变更资金 (Write - Critical)
redis.call("HINCRBY", balanceKey, availField, -changeAmount)
redis.call("HINCRBY", balanceKey, frozenField, changeAmount)

-- 3. 写入发件箱 (Write - Critical)
-- XADD order_events * payload [8888,1001,...]
redis.call("XADD", streamKey, "*", "payload", orderData)

-- 4. 写入订单详情缓存 (Write - Optimization)
-- 使用 SET EX 写入并设置过期时间
-- 这一步即使失败(几乎不可能)也不影响资金安全，但为了原子性放在一起
redis.call("SET", orderKey, orderData, "EX", orderTTL)

return 1 -- 成功
`)
