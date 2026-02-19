// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/ikun2021/gex/app/match/matchservice"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDepthLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetDepthLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDepthLogic {
	return &GetDepthLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetDepthLogic) GetDepth(req *types.GetDepthReq) (resp *types.GetDepthResp, err error) {
	rpcResp, err := l.svcCtx.MatchRpc.GetDepth(l.ctx, &matchservice.GetDepthReq{
		Symbol: req.Symbol,
		Level:  req.Level,
	})
	if err != nil {
		return nil, err
	}

	resp = &types.GetDepthResp{
		Version: rpcResp.Version,
		Asks:    make([]*types.MatchPosition, 0, len(rpcResp.Asks)),
		Bids:    make([]*types.MatchPosition, 0, len(rpcResp.Bids)),
	}

	for _, v := range rpcResp.Asks {
		resp.Asks = append(resp.Asks, &types.MatchPosition{
			BaseAmount:  v.BaseAmount,
			Price:       v.Price,
			QuoteAmount: v.QuoteAmount,
		})
	}
	for _, v := range rpcResp.Bids {
		resp.Bids = append(resp.Bids, &types.MatchPosition{
			BaseAmount:  v.BaseAmount,
			Price:       v.Price,
			QuoteAmount: v.QuoteAmount,
		})
	}

	return
}
