package orderservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

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
	// todo: add your logic here and delete this line

	return nil
}
