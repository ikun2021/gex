package logic

import (
	"context"

	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDepthLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetDepthLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDepthLogic {
	return &GetDepthLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取深度
func (l *GetDepthLogic) GetDepth(in *pb.GetDepthReq) (*pb.GetDepthResp, error) {
	// todo: add your logic here and delete this line

	return &pb.GetDepthResp{}, nil
}
