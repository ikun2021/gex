package orderservicelogic

import (
	"context"
	"errors"
	"fmt"
	logger "github.com/luxun9527/zlog"
	"strings"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

// Redis Stream 的 Key
const StreamKeyMQOutbox = "mq_outbox"

// 订单详情在 Redis 中的过期时间 (秒)
const OrderCacheTTL = 600

// Lua脚本：原子性检查余额、冻结资金、写入Stream、写入缓存
// KEYS[1]: balanceKey (balance:{uid})
// KEYS[2]: streamKey  (mq_outbox)
// KEYS[3]: orderKey   (order:{order_id})
// ARGV[1]: availField (e.g. "USDT")
// ARGV[2]: frozenField (e.g. "USDT_frozen")
// ARGV[3]: changeAmount (int64)
// ARGV[4]: orderData (binary string)
// ARGV[5]: orderTTL (int)
var atomicOrderScript = redis.NewScript(`
local balanceKey = KEYS[1]
local streamKey  = KEYS[2]
local orderKey   = KEYS[3]

local availField   = ARGV[1]
local frozenField  = ARGV[2]
local changeAmount = tonumber(ARGV[3])
local orderData    = ARGV[4]
local orderTTL     = ARGV[5]

-- 1. 检查余额
local currentAvail = tonumber(redis.call("HGET", balanceKey, availField) or "0")
if currentAvail < changeAmount then
    return 0 -- 失败: 余额不足
end

-- 2. 变更资金 (扣减可用，增加冻结)
redis.call("HINCRBY", balanceKey, availField, -changeAmount)
redis.call("HINCRBY", balanceKey, frozenField, changeAmount)

-- 3. 写入发件箱 (Stream)，由 Relayer 异步搬运到 Pulsar
redis.call("XADD", streamKey, "*", "payload", orderData)

-- 4. 写入订单详情缓存 (供查询和幂等检查)
redis.call("SET", orderKey, orderData, "EX", orderTTL)

return 1 -- 成功
`)

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

func (l *CreateOrderLogic) CreateOrder(in *pb.CreateOrderReq) (*pb.OrderEmpty, error) {
	// 1. 解析参数
	price, _ := decimal.NewFromString(in.Price)
	amount, err := decimal.NewFromString(in.BaseAmount)
	if err != nil {
		return nil, errors.New("invalid amount")
	}

	parts := strings.Split(in.SymbolName, "_")
	if len(parts) != 2 {
		return &pb.OrderEmpty{}, errors.New("invalid symbol format")
	}
	baseCcy, quoteCcy := parts[0], parts[1]

	// 2. 计算冻结金额和币种
	var freezeCurrency string
	var freezeAmountInt int64

	if in.Side == enum.Side_Buy {
		// 买入: 冻结 Quote Currency (如 USDT)
		totalCost := price.Mul(amount)
		freezeCurrency = quoteCcy
		freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, totalCost.String())
	} else {
		// 卖出: 冻结 Base Currency (如 BTC)
		freezeCurrency = baseCcy
		freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, amount.String())
	}
	if err != nil {
		return nil, err
	}

	// 3. 生成 ID
	seqId := idgen.NextId()
	orderId := fmt.Sprintf("%d%d%d", int32(in.OrderType), int32(in.Side), seqId)

	// 4. 构建消息体 (Protobuf)
	// 这个数据会存入 Redis Stream 和 Redis String，最终发给撮合引擎
	matchInput := &matchMq.MatchInput{
		Event: &matchMq.MatchInput_CreateOrder{
			CreateOrder: &matchMq.CreateOrderEvent{
				OrderId:     orderId,
				SequenceId:  0, // 此时未定序
				Uid:         in.UserId,
				Side:        in.Side,
				Price:       price.String(),
				BaseAmount:  amount.String(),
				QuoteAmount: in.QuoteAmount,
				OrderType:   in.OrderType,
				SymbolId:    in.SymbolId,
				SymbolName:  in.SymbolName,
			},
		},
		MessageId: seqId,
	}

	// 序列化 Proto
	dataBytes, err := proto.Marshal(matchInput)
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %v", err)
	}
	// Redis String 是二进制安全的，可以直接存储 Proto 字节流
	orderDataStr := string(dataBytes)

	// 5. 准备 Lua 脚本参数
	balanceKey := fmt.Sprintf("balance:%d", in.UserId) // KEYS[1]
	streamKey := StreamKeyMQOutbox                     // KEYS[2]
	orderKey := fmt.Sprintf("order:%s", orderId)       // KEYS[3]

	availField := freezeCurrency              // ARGV[1]
	frozenField := freezeCurrency + "_frozen" // ARGV[2]

	keys := []string{balanceKey, streamKey, orderKey}
	args := []interface{}{
		availField,      // ARGV[1]
		frozenField,     // ARGV[2]
		freezeAmountInt, // ARGV[3]
		orderDataStr,    // ARGV[4] Payload
		OrderCacheTTL,   // ARGV[5] TTL
	}

	// 6. 执行原子脚本
	// 这一步替代了原来的 DB 操作和 Producer.Send
	res, err := atomicOrderScript.Run(l.ctx, l.svcCtx.RedisCli, keys, args...).Result()

	if err != nil {
		logx.Errorw("Execute redis lua failed", logger.ErrorField(err))
		// 区分 Redis 系统错误和业务错误，这里统一返回系统忙
		return nil, errs.CastToDtmError(errs.RedisErr)
	}

	// 脚本返回 0 表示余额不足
	if cast.ToInt64(res) == 0 {
		return nil, errors.New("insufficient balance")
	}

	// 成功：此时资金已冻结，消息已在 Redis Stream 中。
	// 后台的 Relayer 组件会负责将 Stream 中的消息搬运到 Pulsar。
	return &pb.OrderEmpty{}, nil
}
