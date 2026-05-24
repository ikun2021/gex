package account

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/client/accountservice"
	"github.com/ikun2021/gex/app/gateway/internal/ctxdata"
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
	uid, err := ctxdata.GetUid(l.ctx)
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.AccountRpc.GetUserAssetList(l.ctx, &accountservice.GetUserAssetListReq{
		Uid: uid,
	})
	if err != nil {
		l.Logger.Errorf("get user asset list failed: %v", err)
		return nil, err
	}

	assetList := make([]*types.AssetInfo, 0, len(rpcResp.AssetList))
	for _, a := range rpcResp.AssetList {
		assetList = append(assetList, &types.AssetInfo{
			Id:           a.Id,
			CoinName:     a.CoinName,
			CoinID:       a.CoinId,
			AvailableQty: a.AvailableAmount,
			FrozenQty:    a.FrozenAmount,
		})
	}

	return &types.GetUserAssetListResp{
		AssetList: assetList,
	}, nil
}
