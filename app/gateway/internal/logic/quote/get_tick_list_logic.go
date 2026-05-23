package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/ikun2021/gex/app/quote/rpc/quoteservice"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetTickListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetTickListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickListLogic {
	return &GetTickListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetTickListLogic) GetTickList(req *types.GetTickReq) (resp *types.GetTickResp, err error) {
	rpcResp, err := l.svcCtx.QuoteRpc.GetTick(l.ctx, &quoteservice.GetTickReq{
		Symbol: req.Symbol,
		Limit:  req.Limit,
	})
	if err != nil {
		return nil, err
	}

	tickList := make([]*types.TickInfo, 0, len(rpcResp.TickList))
	for _, t := range rpcResp.TickList {
		if t == nil {
			continue
		}
		tickList = append(tickList, &types.TickInfo{
			Price:        t.Price,
			Qty:          t.BaseAmount,
			Amount:       t.QuoteAmount,
			Timestamp:    t.CreatedAt,
			Symbol:       t.Symbol,
			TakerIsBuyer: t.TakerIsBuyer,
		})
	}

	return &types.GetTickResp{TickList: tickList}, nil
}
