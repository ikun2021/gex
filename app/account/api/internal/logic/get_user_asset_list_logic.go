package logic

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/accountservice"
	"github.com/spf13/cast"

	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/app/account/api/internal/types"

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
	uid := l.ctx.Value("uid")
	result, err := l.svcCtx.AccountRpcClient.GetUserAssetList(l.ctx, &accountservice.GetUserAssetListReq{Uid: cast.ToInt64(uid)})
	if err != nil {
		return nil, err
	}
	data := make([]*types.AssetInfo, 0, len(result.AssetList))
	for _, v := range result.AssetList {
		assetInfo := &types.AssetInfo{
			Id:           v.Id,
			CoinName:     v.CoinName,
			CoinID:       v.CoinId,
			AvailableQty: v.AvailableQty,
			FrozenQty:    v.FrozenQty,
		}
		data = append(data, assetInfo)
	}
	resp = &types.GetUserAssetListResp{AssetList: data}
	return
}
