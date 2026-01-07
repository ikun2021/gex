package accountservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeductUserAssetLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeductUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeductUserAssetLogic {
	return &DeductUserAssetLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 扣减用户资产
func (l *DeductUserAssetLogic) DeductUserAsset(in *pb.DeductUserAssetReq) (*pb.Empty, error) {
	// todo: add your logic here and delete this line

	return &pb.Empty{}, nil
}
