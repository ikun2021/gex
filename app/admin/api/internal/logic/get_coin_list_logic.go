package logic

import (
	"context"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetCoinListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetCoinListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetCoinListLogic {
	return &GetCoinListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetCoinListLogic) GetCoinList(req *types.GetCoinListReq) (resp *types.GetCoinListResp, err error) {
	coin := l.svcCtx.AdminQuery.Coin
	offset := (req.PageNo - 1) * req.PageSize
	data, count, err := coin.WithContext(l.ctx).FindByPage(int(offset), int(req.PageSize))
	if err != nil {
		logx.Errorw("getCoinList failed", logx.Field("err", err))
		return nil, err
	}
	coinList := make([]*types.CoinInfo, 0, len(data))
	for _, v := range data {
		d := &types.CoinInfo{
			ID:       v.ID,
			CoinName: v.CoinName,
			Prec:     v.Prec,
			CoinId:   v.CoinID,
		}
		coinList = append(coinList, d)
	}

	return &types.GetCoinListResp{
		List:  coinList,
		Total: count,
	}, nil
}
