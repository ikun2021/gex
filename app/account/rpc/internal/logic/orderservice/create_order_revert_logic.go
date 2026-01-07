package orderservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderRevertLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateOrderRevertLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderRevertLogic {
	return &CreateOrderRevertLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 下单补偿
func (l *CreateOrderRevertLogic) CreateOrderRevert(in *pb.CreateOrderReq) (*pb.OrderEmpty, error) {
	// todo: add your logic here and delete this line

	return &pb.OrderEmpty{}, nil
}
