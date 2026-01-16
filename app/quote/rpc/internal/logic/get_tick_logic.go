package logic

import (
	"context"

	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetTickLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetTickLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickLogic {
	return &GetTickLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取tick
func (l *GetTickLogic) GetTick(in *pb.GetTickReq) (*pb.GetTickResp, error) {
	// todo: add your logic here and delete this line

	return &pb.GetTickResp{}, nil
}
