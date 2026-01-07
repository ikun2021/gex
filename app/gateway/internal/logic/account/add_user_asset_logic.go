package account

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type AddUserAssetLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAddUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddUserAssetLogic {
	return &AddUserAssetLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *AddUserAssetLogic) AddUserAsset(req *types.AddUserAssetReq) (resp *types.Empty, err error) {
	// todo: add your logic here and delete this line

	return
}
