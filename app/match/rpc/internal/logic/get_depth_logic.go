package logic

import (
	"context"

	"github.com/ikun2021/gex/app/match/rpc/internal/svc"
	"github.com/ikun2021/gex/app/match/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDepthLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetDepthLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDepthLogic {
	return &GetDepthLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetDepthLogic) GetDepth(in *pb.GetDepthReq) (*pb.GetDepthResp, error) {
	depth := l.svcCtx.MatchEngine.GetDepth(in.Level)
	ask := make([]*pb.GetDepthResp_Position, 0, len(depth.Asks))
	bid := make([]*pb.GetDepthResp_Position, 0, len(depth.Bids))
	for _, v := range depth.Asks {
		p := &pb.GetDepthResp_Position{
			Qty:    v.Qty,
			Price:  v.Price,
			Amount: v.Amount,
		}
		ask = append(ask, p)
	}
	for _, v := range depth.Bids {
		p := &pb.GetDepthResp_Position{
			Qty:    v.Qty,
			Price:  v.Price,
			Amount: v.Amount,
		}
		bid = append(bid, p)
	}
	return &pb.GetDepthResp{
		Version: depth.CurrentVersion,
		Asks:    ask,
		Bids:    bid,
	}, nil
}
