package logic

import (
	"context"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"

	"github.com/ikun2021/gex/app/match/rpc/internal/svc"
	"github.com/ikun2021/gex/app/match/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetTickLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetTickLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickLogic {
	return &GetTickLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取tick实时成交
func (l *GetTickLogic) GetTick(in *pb.GetTickReq) (*pb.GetTickResp, error) {
	if in.Limit == 0 {
		in.Limit = 40
	}
	matchedOrders, err := l.svcCtx.Query.MatchedOrder.
		WithContext(l.ctx).
		Order(l.svcCtx.Query.MatchedOrder.ID.Desc()).
		Limit(int(in.Limit)).Find()
	if err != nil {
		logx.Errorw("get match order failed", logger.ErrorField(err))
		return nil, err
	}
	tickList := make([]*pb.GetTickResp_Tick, 0, len(matchedOrders))

	for _, v := range matchedOrders {
		f := false
		if v.TakerIsBuyer == 1 {
			f = true
		}
		tick := &pb.GetTickResp_Tick{
			Price:        utils.PrecCut(v.Price, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
			Qty:          utils.PrecCut(v.Qty, l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()),
			Amount:       utils.PrecCut(v.Price, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
			Timestamp:    v.MatchTime,
			Symbol:       v.SymbolName,
			TakerIsBuyer: f,
		}
		tickList = append(tickList, tick)
	}

	return &pb.GetTickResp{TickList: tickList}, nil
}
