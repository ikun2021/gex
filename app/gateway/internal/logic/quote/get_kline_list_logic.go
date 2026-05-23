package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/ikun2021/gex/app/quote/rpc/pb"
	"github.com/ikun2021/gex/app/quote/rpc/quoteservice"

	"github.com/zeromicro/go-zero/core/logx"
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
	rpcResp, err := l.svcCtx.QuoteRpc.GetKline(l.ctx, &quoteservice.GetKlineReq{
		StartTime: req.StartTime,
		EntTime:   req.EndTime,
		KlineType: pb.KlineType(req.KlineType),
		Symbol:    req.Symbol,
	})
	if err != nil {
		return nil, err
	}

	klineList := make([]*types.Kline, 0, len(rpcResp.KlineList))
	for _, k := range rpcResp.KlineList {
		if k == nil {
			continue
		}
		klineList = append(klineList, &types.Kline{
			Open:       k.Open,
			High:       k.High,
			Low:        k.Low,
			Close:      k.Close,
			Amount:     k.Amount,
			Volume:     k.Volume,
			StartTime:  k.StartTime,
			EndTime:    k.EndTime,
			PriceRange: k.Range,
			Symbol:     k.Symbol,
		})
	}

	return &types.KlineListResp{KlineList: klineList}, nil
}
