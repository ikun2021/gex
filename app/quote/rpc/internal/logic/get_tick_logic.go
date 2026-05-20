package logic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ikun2021/gex/app/quote/rpc/internal/dao"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultTickLimit = 100
	maxTickLimit     = 500
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

// GetTick 优先从 Redis 缓存读取最近成交，不足时从 MongoDB 补充。
func (l *GetTickLogic) GetTick(in *pb.GetTickReq) (*pb.GetTickResp, error) {
	if in.Symbol == "" {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "symbol is required")
	}

	limit := int(in.Limit)
	if limit <= 0 {
		limit = defaultTickLimit
	}
	if limit > maxTickLimit {
		limit = maxTickLimit
	}

	ticks, err := l.loadTicksFromRedis(in.Symbol, limit)
	if err != nil {
		l.Logger.Errorf("load tick from redis failed: %v", err)
	}

	if len(ticks) < limit && l.svcCtx.TickRepo != nil {
		mongoTicks, err := l.svcCtx.TickRepo.ListBySymbol(l.ctx, in.Symbol, int64(limit))
		if err != nil {
			l.Logger.Errorf("list tick from mongodb failed: %v", err)
			return nil, fmt.Errorf("list tick failed: %w", err)
		}
		ticks = mergeTicks(ticks, mongoTicks, in.Symbol, limit)
	}

	var total int64
	if l.svcCtx.TickRepo != nil {
		total, _ = l.svcCtx.TickRepo.CountBySymbol(l.ctx, in.Symbol)
	}
	if total == 0 {
		total = int64(len(ticks))
	}

	return &pb.GetTickResp{
		TickList: ticks,
		Total:    total,
	}, nil
}

func (l *GetTickLogic) loadTicksFromRedis(symbol string, limit int) ([]*pb.Tick, error) {
	key := define.Tick.WithParams(symbol)
	items, err := l.svcCtx.RedisClient.Lrange(key, 0, limit-1)
	if err != nil || len(items) == 0 {
		return nil, err
	}

	out := make([]*pb.Tick, 0, len(items))
	for _, item := range items {
		var t commonWs.Tick
		if err := json.Unmarshal([]byte(item), &t); err != nil {
			l.Logger.Errorf("unmarshal cached tick failed: %v", err)
			continue
		}
		out = append(out, wsTickToPb(t, symbol))
	}
	return out, nil
}

// mergeTicks 以 Redis 为准去重合并 MongoDB 数据（按 created_at 降序）。
func mergeTicks(redisList []*pb.Tick, mongoDocs []*dao.TickDoc, symbol string, limit int) []*pb.Tick {
	seen := make(map[int64]struct{}, len(redisList))
	out := make([]*pb.Tick, 0, limit)
	for _, t := range redisList {
		if t == nil {
			continue
		}
		seen[t.CreatedAt] = struct{}{}
		out = append(out, t)
	}
	for _, doc := range mongoDocs {
		if len(out) >= limit {
			break
		}
		if doc == nil {
			continue
		}
		if _, ok := seen[doc.CreatedAt]; ok {
			continue
		}
		out = append(out, tickDocToPb(doc))
	}
	return out
}
