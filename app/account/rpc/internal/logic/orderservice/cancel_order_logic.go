package orderservicelogic

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/common/errs"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/redis/go-redis/v9"
	"github.com/yitter/idgenerator-go/idgen"
	"google.golang.org/protobuf/proto"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

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

// 取消订单逻辑实现 (直发 Pulsar 版)
func (l *CancelOrderLogic) CancelOrder(in *pb.CancelOrderReq) (*pb.OrderEmpty, error) {
	// 1. 获取用户分片标签定位订单快照
	const MaxSlots = 16384
	slotId := in.Uid % MaxSlots
	tag := fmt.Sprintf("{%d}", slotId)
	activeOrdersKey := fmt.Sprintf("orders:active:%s:%d", tag, in.Uid)

	// 2. 从 Redis 校验订单存续性 (确认是可撤单状态)
	orderInfoData, err := l.svcCtx.RedisCli.HGet(l.ctx, activeOrdersKey, in.OrderId).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, errs.OrderNotFound
		}
		return nil, fmt.Errorf("lookup order info failed: %v", err)
	}

	var orderInfo pb.OrderInfo
	if err := proto.Unmarshal(orderInfoData, &orderInfo); err != nil {
		return nil, fmt.Errorf("unmarshal order info failed: %v", err)
	}
	seqId := idgen.NextId()

	// 3. 构建撮合引擎输入消息
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
		return nil, fmt.Errorf("marshal cancel event failed: %v", err)
	}

	// 4. 直接发送至 Pulsar (绕过 Redis Outbox)
	producer, ok := l.svcCtx.MatchProducers[in.SymbolName]
	if !ok {
		return nil, fmt.Errorf("symbol not found in match producers: %s", in.SymbolName)
	}

	_, err = producer.Send(l.ctx, &pulsar.ProducerMessage{
		Payload: streamDataBytes,
		Key:     in.OrderId,
	})
	if err != nil {
		logx.Errorw("Send cancel message direct to pulsar failed",
			logx.Field("orderId", in.OrderId),
			logx.Field("err", err))
		return nil, fmt.Errorf("pulsar send failed")
	}

	return &pb.OrderEmpty{}, nil
}
