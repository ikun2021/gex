package consumer

import (
	"context"
	"fmt"

	"github.com/ikun2021/gex/app/account/rpc/internal/logic"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

// Cancel 删除account订单
func Cancel(sc *svc.ServiceContext, cancelMatchResult *matchMq.MatchOutput_CancelResult, messageId int64) {
	res := cancelMatchResult.CancelResult
	// 1. 获取用户分片标签定位订单快照
	const MaxSlots = 16384
	slotId := res.Uid % MaxSlots
	tag := fmt.Sprintf("{%d}", slotId)
	activeOrdersKey := fmt.Sprintf("orders:active:%s:%d", tag, res.Uid)

	// 2. 构建订单 ID
	orderId := fmt.Sprintf("%d%d%d", int32(enum.OrderType_LO), int32(res.Side), res.Id)

	// 3. 尝试从 Hash 中获取订单信息，以确定 symbolName
	ctx := context.Background()
	var symbolName string
	val, err := sc.RedisCli.HGet(ctx, activeOrdersKey, orderId).Bytes()
	if err == nil {
		var orderInfo pb.OrderInfo
		if err := proto.Unmarshal(val, &orderInfo); err == nil {
			symbolName = orderInfo.SymbolName
		}
	} else {
		// 如果 LO 没找到，尝试 MO (虽然 MO 通常不在 active orders 停留)
		orderIdMO := fmt.Sprintf("%d%d%d", int32(enum.OrderType_MO), int32(res.Side), res.Id)
		val, err = sc.RedisCli.HGet(ctx, activeOrdersKey, orderIdMO).Bytes()
		if err == nil {
			orderId = orderIdMO
			var orderInfo pb.OrderInfo
			if err := proto.Unmarshal(val, &orderInfo); err == nil {
				symbolName = orderInfo.SymbolName
			}
		}
	}

	// 4. 调用逻辑层处理资产解冻和 ZSet 索引清理
	l := logic.NewHandleMatchResultLogic(sc)
	if err := l.HandleCancelOrder(res, messageId, symbolName, func() error { return nil }); err != nil {
		logx.Errorw("HandleCancelOrder failed",
			logx.Field("uid", res.Uid),
			logx.Field("err", err))
	}

	// 5. 从 Redis 删除活跃订单快照
	_, err = sc.RedisCli.HDel(ctx, activeOrdersKey, orderId).Result()
	if err != nil {
		logx.Errorw("delete active order snapshot failed",
			logx.Field("uid", res.Uid),
			logx.Field("orderId", orderId),
			logx.Field("err", err))
		return
	}

	logx.Infow("cancel order success and removed from redis",
		logx.Field("uid", res.Uid),
		logx.Field("orderId", orderId),
		logx.Field("symbolName", symbolName))
}
