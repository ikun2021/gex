package account

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserAssetListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetUserAssetListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserAssetListLogic {
	return &GetUserAssetListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserAssetListLogic) GetUserAssetList() (resp *types.GetUserAssetListResp, err error) {
	// todo: add your logic here and delete this line

	return
}
