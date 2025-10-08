package logic

import (
	"context"
	matchpb "github.com/ikun2021/gex/app/match/rpc/pb"
	"github.com/ikun2021/gex/app/quotes/api/internal/svc"
	"github.com/ikun2021/gex/app/quotes/api/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/spf13/cast"
	"google.golang.org/grpc/metadata"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDepthListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetDepthListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDepthListLogic {
	return &GetDepthListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetDepthListLogic) GetDepthList(req *types.GetDepthListReq) (resp *types.GetDepthListResp, err error) {
	_, ok := l.svcCtx.Symbols.Load(req.Symbol)
	if !ok {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "symbol not existed")
	}
	ctx := metadata.NewIncomingContext(l.ctx, metadata.Pairs("symbol", req.Symbol))
	depthResp, err := l.svcCtx.MatchClients.GetDepth(ctx, &matchpb.GetDepthReq{
		Symbol: req.Symbol,
		Level:  req.Level,
	})
	if err != nil {
		logx.Errorf("GetDepthListLogic.GetDepthList error:%v", err)
		return nil, err
	}
	asks := make([]*types.Position, 0, len(depthResp.Asks))
	bids := make([]*types.Position, 0, len(depthResp.Bids))
	for _, v := range depthResp.Asks {
		position := &types.Position{
			Qty:    v.Qty,
			Price:  v.Price,
			Amount: v.Amount,
		}
		asks = append(asks, position)
	}
	for _, v := range depthResp.Bids {
		position := &types.Position{
			Qty:    v.Qty,
			Price:  v.Price,
			Amount: v.Amount,
		}
		bids = append(bids, position)
	}
	resp = &types.GetDepthListResp{
		Version: cast.ToString(depthResp.Version),
		Asks:    asks,
		Bids:    bids,
	}
	return
}
