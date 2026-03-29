package order

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/client/orderservice"
	"github.com/ikun2021/gex/common/proto/enum"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderLogic {
	return &CreateOrderLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateOrderLogic) CreateOrder(req *types.CreateOrderReq) (resp *types.Empty, err error) {
	_, err = l.svcCtx.OrderRpc.CreateOrder(l.ctx, &orderservice.CreateOrderReq{
		UserId:      1,
		SymbolId:    1,
		SymbolName:  req.SymbolName,
		BaseAmount:  req.BaseAmount,
		Price:       req.Price,
		QuoteAmount: req.QuoteAmount,
		Side:        enum.Side(req.Side),
		OrderType:   enum.OrderType(req.OrderType),
	})
	if err != nil {
		l.Logger.Errorf("create order failed: %v", err)
	}
	return &types.Empty{}, err

}
