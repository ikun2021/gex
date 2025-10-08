package logic

import (
	"context"
	"github.com/ikun2021/gex/app/match/rpc/pb"
	"github.com/ikun2021/gex/app/quotes/api/internal/svc"
	"github.com/ikun2021/gex/app/quotes/api/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/metadata"
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
	_, ok := l.svcCtx.Symbols.Load(req.Symbol)
	if !ok {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "symbol not existed")
	}
	ctx := metadata.NewIncomingContext(l.ctx, metadata.Pairs("symbol", req.Symbol))
	tickerResp, err := l.svcCtx.MatchClients.GetTicker(ctx, &pb.GetTickerReq{Symbol: req.Symbol})
	if err != nil {
		logx.Errorw("GetTickerList error", logx.Field("symbol", req.Symbol), logx.Field("err", err))
		return nil, err
	}
	tickerList := make([]*types.Ticker, 0, len(tickerResp.TickerList))
	for _, v := range tickerResp.TickerList {
		ticker := &types.Ticker{
			LastPrice:   v.LatestPrice,
			High:        v.High,
			Low:         v.Low,
			Volume:      v.Volume,
			Amount:      v.Amount,
			PriceRange:  v.PriceRange,
			Symbol:      v.Symbol,
			Last24Price: v.Last24Price,
		}
		tickerList = append(tickerList, ticker)
	}

	resp = &types.GetTickerListResp{}
	resp.TickerList = tickerList

	return
}
