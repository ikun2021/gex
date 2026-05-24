package accountservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type FreezeUserAssetLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFreezeUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FreezeUserAssetLogic {
	return &FreezeUserAssetLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 冻结用户资产。
func (l *FreezeUserAssetLogic) FreezeUserAsset(in *pb.FreezeUserAssetReq) (*pb.Empty, error) {

	return &pb.Empty{}, nil
}
