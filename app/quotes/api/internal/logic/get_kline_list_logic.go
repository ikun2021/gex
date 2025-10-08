package logic

import (
	"context"
	"github.com/ikun2021/gex/app/quotes/api/internal/svc"
	"github.com/ikun2021/gex/app/quotes/api/internal/types"
	klinepb "github.com/ikun2021/gex/app/quotes/kline/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/metadata"
)

type GetKlineListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetKlineListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetKlineListLogic {
	return &GetKlineListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetKlineListLogic) GetKlineList(req *types.KlineListReq) (resp *types.KlineListResp, err error) {
	_, ok := l.svcCtx.Symbols.Load(req.Symbol)
	if !ok {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "symbol not existed")
	}
	ctx := metadata.NewIncomingContext(l.ctx, metadata.Pairs("symbol", req.Symbol))
	klineResp, err := l.svcCtx.KlineClients.GetKline(ctx, &klinepb.GetKlineReq{
		StartTime: req.StartTime,
		EntTime:   req.EndTime,
		KlineType: klinepb.KlineType(req.KlineType),
		Symbol:    req.Symbol,
	})
	if err != nil {
		logx.Errorf("get kline list error: %v", err)
		return nil, err
	}
	klines := make([]*types.Kline, 0, len(klineResp.KlineList))
	for _, v := range klineResp.KlineList {
		k := &types.Kline{
			Open:       v.Open,
			High:       v.High,
			Low:        v.Low,
			Close:      v.Close,
			Volume:     v.Volume,
			Amount:     v.Amount,
			StartTime:  v.StartTime,
			EndTime:    v.EndTime,
			PriceRange: v.Range,
			Symbol:     v.Symbol,
		}
		klines = append(klines, k)
	}
	resp = &types.KlineListResp{}
	resp.KlineList = klines
	return
}
