package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type GetTickerLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetTickerLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickerLogic {
	return &GetTickerLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetTicker 从 Redis 读取 24 小时行情快照。
func (l *GetTickerLogic) GetTicker(in *pb.GetTickerReq) (*pb.GetTickerResp, error) {
	if in.Symbol == "" {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "symbol is required")
	}

	data, err := l.svcCtx.RedisClient.Hget(define.Ticker.WithParams(), in.Symbol)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return &pb.GetTickerResp{}, nil
		}
		l.Logger.Errorf("get ticker from redis failed: %v", err)
		return nil, fmt.Errorf("get ticker failed: %w", err)
	}
	if data == "" {
		return &pb.GetTickerResp{}, nil
	}

	var ticker commonWs.Ticker
	if err := json.Unmarshal([]byte(data), &ticker); err != nil {
		l.Logger.Errorf("unmarshal ticker failed: %v", err)
		return nil, fmt.Errorf("unmarshal ticker failed: %w", err)
	}

	return &pb.GetTickerResp{Ticker: wsTickerToPb(ticker)}, nil
}
