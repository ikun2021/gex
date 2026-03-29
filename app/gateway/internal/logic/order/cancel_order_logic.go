package order

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/client/orderservice"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type CancelOrderLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCancelOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CancelOrderLogic {
	return &CancelOrderLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CancelOrderLogic) CancelOrder(req *types.CancelOrderReq) (resp *types.Empty, err error) {
	_, err = l.svcCtx.OrderRpc.CancelOrder(l.ctx, &orderservice.CancelOrderReq{
		OrderId:    req.ID,
		Uid:        1,
		SymbolName: req.SymbolName,
	})
	if err != nil {
		l.Logger.Errorf("cancel order failed: %v", err)
	}
	return &types.Empty{}, err

}
