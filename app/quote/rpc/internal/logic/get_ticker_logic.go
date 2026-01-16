package logic

import (
	"context"

	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetTickerLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetTickerLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickerLogic {
	return &GetTickerLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取24小时行情
func (l *GetTickerLogic) GetTicker(in *pb.GetTickerReq) (*pb.GetTickerResp, error) {
	// todo: add your logic here and delete this line

	return &pb.GetTickerResp{}, nil
}
