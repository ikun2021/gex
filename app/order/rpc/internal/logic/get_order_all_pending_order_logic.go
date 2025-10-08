package logic

import (
	"context"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/model"
	"github.com/ikun2021/gex/common/proto/enum"
	commonUtils "github.com/ikun2021/gex/common/utils"

	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	"github.com/ikun2021/gex/app/order/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetOrderAllPendingOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetOrderAllPendingOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetOrderAllPendingOrderLogic {
	return &GetOrderAllPendingOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取所有订单状态为未成交或部分成交的订单
func (l *GetOrderAllPendingOrderLogic) GetOrderAllPendingOrder(in *pb.OrderEmpty, stream pb.OrderService_GetOrderAllPendingOrderServer) error {
	for i := 0; i < commonUtils.ShardingCount; i++ {
		for j := 0; true; j++ {
			entrustOrder := l.svcCtx.Query.EntrustOrder.Table(commonUtils.WithShardingSuffix(model.TableNameEntrustOrder, int64(i)))
			result, _, err := entrustOrder.WithContext(l.ctx).
				Where(entrustOrder.Status.In(int32(enum.OrderStatus_NewCreated), int32(enum.OrderStatus_PartFilled)),
					entrustOrder.SymbolID.Eq(l.svcCtx.Config.SymbolInfo.SymbolID)).
				Order(entrustOrder.ID).
				FindByPage(j*1000, 1000)
			if len(result) == 0 {
				break
			}
			if err != nil {
				logx.Errorw("query pending order failed", logx.Field("err", err))
				break
			}
			for _, v := range result {
				d := &pb.GetOrderAllPendingOrderResp{
					OrderId:        v.OrderID,
					SequenceId:     v.ID,
					Uid:            v.UserID,
					Side:           enum.Side(v.Side),
					Price:          v.Price,
					Qty:            v.Qty,
					Amount:         v.Amount,
					OrderType:      enum.OrderType(v.OrderType),
					UnFilledAmount: v.UnFilledAmount,
					UnFilledQty:    v.UnFilledQty,
				}
				if err := stream.Send(d); err != nil {
					logx.Errorw("send order to match failed", logx.Field("err", err))
				}

			}
		}
	}

	//发送结束
	d := &pb.GetOrderAllPendingOrderResp{
		Done: true,
	}
	if err := stream.Send(d); err != nil {
		logx.Errorw("send order to match failed", logx.Field("err", err))
	}
	return nil
}
