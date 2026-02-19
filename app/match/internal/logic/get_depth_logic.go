package logic

import (
	"context"
	"fmt"

	"github.com/ikun2021/gex/app/match/internal/handler"
	"github.com/ikun2021/gex/app/match/internal/svc"
	"github.com/ikun2021/gex/app/match/pb"

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

// 获取深度
func (l *GetDepthLogic) GetDepth(in *pb.GetDepthReq) (*pb.GetDepthResp, error) {
	s, ok := handler.Handlers[in.Symbol]
	if !ok {
		return &pb.GetDepthResp{}, fmt.Errorf(`not found symbol: %s`, in.Symbol)
	}
	depth := s.DepthHandler.GetDepth(in.Level)
	ask := make([]*pb.GetDepthResp_Position, 0, len(depth.Asks))
	bid := make([]*pb.GetDepthResp_Position, 0, len(depth.Bids))
	for _, v := range depth.Asks {
		p := &pb.GetDepthResp_Position{
			BaseAmount:  v.Qty,
			Price:       v.Price,
			QuoteAmount: v.Amount,
		}
		ask = append(ask, p)
	}
	for _, v := range depth.Bids {
		p := &pb.GetDepthResp_Position{
			BaseAmount:  v.Qty,
			Price:       v.Price,
			QuoteAmount: v.Amount,
		}
		bid = append(bid, p)
	}
	return &pb.GetDepthResp{
		Version: depth.CurrentVersion,
		Asks:    ask,
		Bids:    bid,
	}, nil

}
