package logic

import (
	"context"
	"github.com/ikun2021/gex/common/proto/define"
	logger "github.com/luxun9527/zlog"
	"gopkg.in/yaml.v3"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type SyncSymbolConfigLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSyncSymbolConfigLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SyncSymbolConfigLogic {
	return &SyncSymbolConfigLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SyncSymbolConfigLogic) SyncSymbolConfig(req *types.Empty) (resp *types.Empty, err error) {
	symbol := l.svcCtx.AdminQuery.Symbol

	symbols, err := symbol.WithContext(l.ctx).Find()
	if err != nil {
		logx.Errorw("sync symbol config failed", logger.ErrorField(err))
		return &types.Empty{}, nil
	}
	for _, v := range symbols {
		symbolInfo := &define.SymbolInfo{
			SymbolName:         v.SymbolName,
			SymbolID:           v.SymbolID,
			BaseCoinName:       v.BaseCoinName,
			BaseCoinID:         int32(v.BaseCoinID),
			QuoteCoinName:      v.QuoteCoinName,
			QuoteCoinID:        int32(v.QuoteCoinID),
			BaseCoinPrecValue:  v.BaseCoinPrec,
			QuoteCoinPrecValue: v.QuoteCoinPrec,
		}
		data, err := yaml.Marshal(symbolInfo)
		if err != nil {
			logx.Errorw("yaml marshal config failed", logx.Field("err", err))
			return nil, err
		}
		if _, err := l.svcCtx.EtcdCli.Put(l.ctx, define.EtcdSymbolPrefix+v.SymbolName, string(data)); err != nil {
			logx.Errorw("put config to etcd failed", logx.Field("err", err))
			return nil, err
		}
	}

	return
}
