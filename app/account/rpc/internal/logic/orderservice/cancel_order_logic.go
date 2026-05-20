package orderservicelogic

import (
	"context"
	"errors"
	"fmt"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/rediskeys"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"google.golang.org/protobuf/proto"

	"github.com/zeromicro/go-zero/core/logx"
)

// Lua：原子撤单（幂等检查 + CAS 校验快照 + 写入 Outbox + 删除 active/open）。
// KEYS[1]: activeOrdersKey  KEYS[2]: openOrdersKey  KEYS[3]: streamKey  KEYS[4]: idempotencyKey
// ARGV[1]: orderId  ARGV[2]: expectedOrderData  ARGV[3]: streamPayload  ARGV[4]: expireSec
// 返回 1=成功, 0=订单不存在, -1=重复请求, -2=快照已变更, -3=stream写入失败
var atomicCancelOrderScript = redis.NewScript(`
local orderId = ARGV[1]
if redis.call("EXISTS", KEYS[4]) == 1 then
    return -1
end
local data = redis.call("HGET", KEYS[1], orderId)
if not data then
    return 0
end
if ARGV[2] ~= "" and data ~= ARGV[2] then
    return -2
end
local streamOk = redis.call("XADD", KEYS[3], "*", "payload", ARGV[3], "oid", orderId)
if not streamOk then
    return -3
end
redis.call("HDEL", KEYS[1], orderId)
redis.call("ZREM", KEYS[2], orderId)
redis.call("SET", KEYS[4], "1", "EX", ARGV[4])
return 1
`)

type CancelOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCancelOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CancelOrderLogic {
	return &CancelOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// CancelOrder 先校验订单状态，再通过 Lua 原子写入 Outbox 并删除 active/open，由 Relayer 投递撮合。
func (l *CancelOrderLogic) CancelOrder(in *pb.CancelOrderReq) (*pb.OrderEmpty, error) {
	tag := rediskeys.UserSlotTag(in.Uid)
	activeOrdersKey := rediskeys.UserActiveOrdersKey(tag, in.Uid)

	orderInfoData, err := l.svcCtx.RedisCli.HGet(l.ctx, activeOrdersKey, in.OrderId).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, errs.OrderNotFound
		}
		return nil, fmt.Errorf("lookup order info failed: %w", err)
	}

	var orderInfo pb.OrderInfo
	if err := proto.Unmarshal(orderInfoData, &orderInfo); err != nil {
		return nil, fmt.Errorf("unmarshal order info failed: %w", err)
	}

	switch orderInfo.Status {
	case enum.OrderStatus_NewCreated, enum.OrderStatus_PartFilled:
	default:
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "order cannot be canceled")
	}

	seqId := idgen.NextId()
	matchInput := &matchMq.MatchInput{
		Event: &matchMq.MatchInput_CancelOrder{
			CancelOrder: &matchMq.CancelOrderEvent{
				Id:        orderInfo.Id,
				Price:     orderInfo.Price,
				Side:      orderInfo.Side,
				OrderType: orderInfo.OrderType,
			},
		},
		MessageId:  seqId,
		SymbolName: in.SymbolName,
	}
	streamDataBytes, err := proto.Marshal(matchInput)
	if err != nil {
		return nil, fmt.Errorf("marshal cancel event failed: %w", err)
	}

	openOrdersKey := rediskeys.UserOpenOrdersKey(tag, in.Uid)
	streamKey := fmt.Sprintf("%s_%s:%s", StreamKeyMQOutbox, in.SymbolName, tag)
	idempotencyKey := fmt.Sprintf("req:cancel:%s:%s", in.OrderId, tag)

	res, err := atomicCancelOrderScript.Run(l.ctx, l.svcCtx.RedisCli,
		[]string{activeOrdersKey, openOrdersKey, streamKey, idempotencyKey},
		in.OrderId, string(orderInfoData), string(streamDataBytes), 86400,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("atomic cancel order failed: %w", err)
	}

	switch cast.ToInt64(res) {
	case 1, -1:
		logx.Infow("cancel order enqueued",
			logx.Field("orderId", in.OrderId),
			logx.Field("uid", in.Uid),
			logx.Field("symbol", in.SymbolName))
		return &pb.OrderEmpty{}, nil
	case 0:
		return nil, errs.OrderNotFound
	case -2:
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "order state changed, retry cancel")
	case -3:
		return nil, fmt.Errorf("mq outbox write failed")
	default:
		return nil, fmt.Errorf("unexpected cancel script result: %v", res)
	}
}
