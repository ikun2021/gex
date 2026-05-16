package orderservicelogic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/rediskeys"

	"time"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
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

// Lua脚本详情：
// 1. 原子性：检查余额、扣减资金、写入发件箱、记录快照在一个事务中完成。
// 2. 幂等性：使用 idempotencyKey 防止重复下单。
// 3. 集群兼容：通过 {slotId} hash-tag 确保相关的所有 Key 落在同一个分片上。
// KEYS: [balanceKey, streamKey, activeOrdersKey, idempotencyKey, openOrdersKey]
// ARGV: [availField, frozenField, amount, streamData, orderId, orderInfoData, expireTime, seqId]
var atomicOrderScript = redis.NewScript(`
local balanceKey      = KEYS[1]
local streamKey       = KEYS[2]
local activeOrdersKey = KEYS[3]
local idempotencyKey  = KEYS[4]
local openOrdersKey   = KEYS[5]

local availField      = ARGV[1]
local frozenField     = ARGV[2]
local changeAmount    = tonumber(ARGV[3])
local streamData      = ARGV[4]
local orderId         = ARGV[5]
local orderInfoData   = ARGV[6]
local expireTime      = ARGV[7]
local seqId           = ARGV[8]

-- 1. 幂等检查
if redis.call("EXISTS", idempotencyKey) == 1 then
    return -1
end

-- 2. 预检查类型
local balanceType = redis.call("TYPE", balanceKey).ok
if balanceType ~= "hash" and balanceType ~= "none" then
    return -3
end

-- 3. 余额校验
local currentAvail = tonumber(redis.call("HGET", balanceKey, availField) or "0")
if currentAvail < changeAmount then
    return -2
end

-- 4. 执行变更 (原子操作)
-- 4.1 写入发件箱 (Stream)
local streamOk = redis.call("XADD", streamKey, "*", "payload", streamData, "oid", orderId)
if not streamOk then
    return -4
end

-- 4.2 变更资金 (扣减可用，增加冻结)
redis.call("HINCRBY", balanceKey, availField, -changeAmount)
redis.call("HINCRBY", balanceKey, frozenField, changeAmount)

-- 4.3 写入活跃订单快照
redis.call("HSET", activeOrdersKey, orderId, orderInfoData)

-- 4.4 写入有序索引（score 为雪花 id，便于按 id 游标分页）
redis.call("ZADD", openOrdersKey, seqId, orderId)

-- 4.5 标记幂等
redis.call("SET", idempotencyKey, "1", "EX", expireTime)

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

	if in.OrderType != enum.OrderType_MO && in.OrderType != enum.OrderType_LO {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "order type is invalid")
	}
	var (
		price, baseAmount, quoteAmount decimal.Decimal
		freezeCurrency                 string
		freezeAmountInt                int64
	)
	price, err := decimal.NewFromString(in.Price)
	if err != nil {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "price invalid")
	}

	parts := strings.Split(in.SymbolName, "_")
	if len(parts) != 2 {
		return &pb.OrderEmpty{}, errors.New("invalid symbol format")
	}
	baseCcy, quoteCcy := parts[0], parts[1]

	price, err = decimal.NewFromString(in.Price)
	if err != nil {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "price invalid")
	}
	baseAmount, err = decimal.NewFromString(in.BaseAmount)
	if err != nil {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "baseAmount invalid")
	}

	if in.OrderType == enum.OrderType_LO {
		if in.Side == enum.Side_Buy {
			// 买入: 冻结 Quote Currency (如 USDT)
			freezeCurrency = quoteCcy
			quoteAmount = price.Mul(baseAmount)
			freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, quoteAmount.String())
		} else {
			// 卖出: 冻结 Base Currency (如 BTC)
			freezeCurrency = baseCcy
			freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, baseAmount.String())
		}
	} else {
		if in.Side == enum.Side_Buy {
			// 买入: 冻结 Quote Currency (如 USDT)
			freezeCurrency = quoteCcy
			freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, quoteAmount.String())
		} else {
			// 卖出: 冻结 Base Currency (如 BTC)
			freezeCurrency = baseCcy
			freezeAmountInt, err = utils.ToDBInteger(freezeCurrency, baseAmount.String())
		}
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
				SequenceId:  seqId, // 此时未定序
				Uid:         in.UserId,
				Side:        in.Side,
				Price:       price.String(),
				BaseAmount:  baseAmount.String(),
				QuoteAmount: quoteAmount.String(),
				OrderType:   in.OrderType,
				SymbolId:    in.SymbolId,
				SymbolName:  in.SymbolName,
			},
		},
		MessageId: seqId,
	}

	// 序列化 Proto (Stream 端)
	streamDataBytes, err := proto.Marshal(matchInput)
	if err != nil {
		return nil, fmt.Errorf("marshal matchInput failed: %v", err)
	}
	streamDataStr := string(streamDataBytes)

	// 5. 构建 OrderInfo (Redis 快照端)
	now := time.Now().UnixMilli()
	orderInfo := &pb.OrderInfo{
		Id:                  seqId,
		OrderId:             orderId,
		UserId:              in.UserId,
		SymbolId:            in.SymbolId,
		SymbolName:          in.SymbolName,
		BaseAmount:          baseAmount.String(),
		Price:               price.String(),
		Side:                in.Side,
		QuoteAmount:         in.QuoteAmount,
		Status:              enum.OrderStatus_NewCreated,
		OrderType:           in.OrderType,
		FilledBaseAmount:    "0",
		UnFilledBaseAmount:  baseAmount.String(),
		FilledAvgPrice:      "0",
		FilledQuoteAmount:   "0",
		UnFilledQuoteAmount: in.QuoteAmount,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	orderInfoDataBytes, err := proto.Marshal(orderInfo)
	if err != nil {
		return nil, fmt.Errorf("marshal orderInfo failed: %v", err)
	}
	orderInfoDataStr := string(orderInfoDataBytes)

	// 6. 准备 Lua 脚本参数 (使用 slotId 分桶 HashTag 保证 Redis Cluster 集群原子性并限制 Stream 数量)
	tag := rediskeys.UserSlotTag(in.UserId)

	balanceKey := rediskeys.UserBalanceKey(tag, in.UserId)
	activeOrdersKey := rediskeys.UserActiveOrdersKey(tag, in.UserId)
	openOrdersKey := rediskeys.UserOpenOrdersKey(tag, in.UserId)
	streamKey := fmt.Sprintf("%s_%s:%s", StreamKeyMQOutbox, in.SymbolName, tag) // 同一 Slot 的分桶 Stream
	idempotencyKey := fmt.Sprintf("req:order:%s:%s", orderId, tag)

	keys := []string{balanceKey, streamKey, activeOrdersKey, idempotencyKey, openOrdersKey}
	args := []interface{}{
		freezeCurrency,             // ARGV[1] availField
		freezeCurrency + "_frozen", // ARGV[2] frozenField
		freezeAmountInt,            // ARGV[3] amount
		streamDataStr,              // ARGV[4] streamData
		orderId,                    // ARGV[5] orderId
		orderInfoDataStr,           // ARGV[6] orderInfoData
		86400,                      // ARGV[7] expireTime
		seqId,                      // ARGV[8] seqId
	}

	res, err := atomicOrderScript.Run(context.Background(), l.svcCtx.RedisCli, keys, args...).Result()
	if err != nil {
		logx.Errorw("Execute redis lua failed", logx.Field("err", err))
		return nil, err
	}

	// 7. 处理返回结果
	resultCode := cast.ToInt64(res)
	switch resultCode {
	case 1:
		// 成功：原子操作完成 (资产扣减 + Stream 写入)
		return &pb.OrderEmpty{}, nil
	case -1:
		return &pb.OrderEmpty{}, nil
	case -2:
		return nil, errs.AmountInsufficient
	case -3:
		return nil, fmt.Errorf("redis type conflict")
	case -4:
		return nil, fmt.Errorf("mq stream write failed")
	default:
		return nil, fmt.Errorf("unexpected script result: %d", resultCode)
	}
}
