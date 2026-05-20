package logic

import (
	"context"
	"fmt"

	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/rpc/pb"
	"github.com/ikun2021/gex/common/errs"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetKlineLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetKlineLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetKlineLogic {
	return &GetKlineLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetKline 从 MongoDB 查询历史 K 线，并合并 Redis 中当前未落库的 K 线。
func (l *GetKlineLogic) GetKline(in *pb.GetKlineReq) (*pb.GetKlineResp, error) {
	if in.Symbol == "" {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "symbol is required")
	}
	if in.KlineType == pb.KlineType_Unknown {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "kline_type is required")
	}

	var list []*pb.GetKlineResp_Kline
	if l.svcCtx.KlineHistoryRepo != nil {
		docs, err := l.svcCtx.KlineHistoryRepo.ListByRange(
			l.ctx, in.Symbol, int32(in.KlineType), in.StartTime, in.GetEntTime())
		if err != nil {
			l.Logger.Errorf("list kline from mongodb failed: %v", err)
			return nil, fmt.Errorf("list kline failed: %w", err)
		}
		list = make([]*pb.GetKlineResp_Kline, 0, len(docs)+1)
		for _, doc := range docs {
			list = append(list, historyToPbKline(doc))
		}
	}

	latest, err := loadLatestKlineFromRedis(l.svcCtx.RedisClient, in.Symbol, in.KlineType)
	if err != nil {
		l.Logger.Errorf("load latest kline from redis failed: %v", err)
	} else if latest != nil {
		if in.GetEntTime() == 0 || latest.StartTime <= in.GetEntTime() {
			if in.StartTime == 0 || latest.StartTime >= in.StartTime {
				list = mergeLatestKline(list, latest)
			}
		}
	}

	return &pb.GetKlineResp{KlineList: list}, nil
}
