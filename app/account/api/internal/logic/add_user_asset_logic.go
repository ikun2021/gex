package logic

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/accountservice"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"

	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/app/account/api/internal/types"

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

func (l *AddUserAssetLogic) AddUserAsset(in *types.AddUserAssetReq) (resp *types.Empty, err error) {
	uid := l.ctx.Value("uid")

	_, err = l.svcCtx.AccountRpcClient.AddUserAsset(l.ctx, &accountservice.AddUserAssetReq{
		Uid:      cast.ToInt64(uid),
		CoinName: in.CoinName,
		Qty:      in.Qty,
	})
	if err != nil {
		logx.Errorw("call AddUserAsset login failed", logger.ErrorField(err))
		return nil, err
	}

	return
}
