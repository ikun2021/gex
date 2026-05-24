package accountservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type UnFreezeUserAssetLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnFreezeUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnFreezeUserAssetLogic {
	return &UnFreezeUserAssetLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 解冻用户资产
func (l *UnFreezeUserAssetLogic) UnFreezeUserAsset(in *pb.UnFreezeUserAssetReq) (*pb.Empty, error) {
	// todo: add your logic here and delete this line

	return &pb.Empty{}, nil
}
