package accountservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserAssetByCoinLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserAssetByCoinLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserAssetByCoinLogic {
	return &GetUserAssetByCoinLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取用户指定币种的资产
func (l *GetUserAssetByCoinLogic) GetUserAssetByCoin(in *pb.GetUserAssetReq) (*pb.GetUserAssetResp, error) {
	return nil, nil
}
