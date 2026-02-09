package accountservicelogic

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/zeromicro/go-zero/core/logx"
)

type AddUserAssetLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAddUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddUserAssetLogic {
	return &AddUserAssetLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 增加用户资产
func (l *AddUserAssetLogic) AddUserAsset(in *pb.AddUserAssetReq) (*pb.Empty, error) {

	return &pb.Empty{}, nil
}
