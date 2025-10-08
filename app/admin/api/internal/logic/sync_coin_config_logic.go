package logic

import (
	"context"
	"github.com/ikun2021/gex/common/proto/define"
	"gopkg.in/yaml.v3"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type SyncCoinConfigLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSyncCoinConfigLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SyncCoinConfigLogic {
	return &SyncCoinConfigLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SyncCoinConfigLogic) SyncCoinConfig(req *types.Empty) (resp *types.Empty, err error) {
	coin := l.svcCtx.AdminQuery.Coin

	coins, err := coin.WithContext(l.ctx).Find()
	if err != nil {
		return &types.Empty{}, nil
	}
	for _, v := range coins {
		coinInfo := &define.CoinInfo{
			CoinID:   v.CoinID,
			CoinName: v.CoinName,
			Prec:     v.Prec,
		}
		data, err := yaml.Marshal(coinInfo)
		if err != nil {
			logx.Errorw("marshal config to yaml failed", logx.Field("err", err))
			return &types.Empty{}, nil
		}
		if _, err := l.svcCtx.EtcdCli.Put(l.ctx, define.EtcdCoinPrefix+v.CoinName, string(data)); err != nil {
			logx.Errorw("put config to etcd failed", logx.Field("err", err))
			return &types.Empty{}, nil
		}
	}

	return
}
