package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/ikun2021/gex/app/quote/rpc/quoteservice"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetTickerListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetTickerListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickerListLogic {
	return &GetTickerListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetTickerListLogic) GetTickerList(req *types.GetTickerListReq) (resp *types.GetTickerListResp, err error) {
	rpcResp, err := l.svcCtx.QuoteRpc.GetTicker(l.ctx, &quoteservice.GetTickerReq{
		Symbol: req.Symbol,
	})
	if err != nil {
		return nil, err
	}

	tickerList := make([]*types.Ticker, 0, 1)
	if rpcResp.Ticker != nil {
		t := rpcResp.Ticker
		tickerList = append(tickerList, &types.Ticker{
			LastPrice:   t.Close,
			High:        t.High,
			Low:         t.Low,
			Amount:      t.Amount,
			Volume:      t.Volume,
			PriceRange:  t.Range,
			Last24Price: t.Open,
			Symbol:      t.Symbol,
		})
	}

	return &types.GetTickerListResp{TickerList: tickerList}, nil
}
