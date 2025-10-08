package logic

import (
	"context"
	orderpb "github.com/ikun2021/gex/app/order/rpc/pb"
	"github.com/spf13/cast"
	"google.golang.org/grpc/metadata"

	"github.com/ikun2021/gex/app/order/api/internal/svc"
	"github.com/ikun2021/gex/app/order/api/internal/types"

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
	ctx := metadata.NewIncomingContext(l.ctx, metadata.Pairs("symbol", cast.ToString(req.SymbolName)))
	uid := l.ctx.Value("uid")
	_, err = l.svcCtx.OrderClient.CancelOrder(ctx, &orderpb.CancelOrderReq{
		Id:  cast.ToInt64(req.ID),
		Uid: cast.ToInt64(uid),
	})
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil

}
