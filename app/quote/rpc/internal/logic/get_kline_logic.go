package logic

import (
	"context"
	"encoding/json"
	"errors"
	m "github.com/ikun2021/gex/app/quote/kline/rpc/internal/model"
	"github.com/ikun2021/gex/app/quote/kline/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/kline/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"

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

// 获取k线
func (l *GetKlineLogic) GetKline(in *pb.GetKlineReq) (*pb.GetKlineResp, error) {
	quoteCoinPrec := l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()
	baseCoinPrec := l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()
	//从数据库查历史k线
	kline := l.svcCtx.Query.Kline
	klineList, err := kline.WithContext(l.ctx).
		Where(kline.Symbol.Eq(l.svcCtx.Config.SymbolInfo.SymbolName), kline.KlineType.Eq(int32(in.KlineType)), kline.StartTime.Between(in.StartTime, in.EntTime)).
		Limit(300).
		Order(kline.StartTime.Desc()).
		Find()
	if err != nil {
		logx.Errorw("query  kline from mysql failed", logger.ErrorField(err))
		return nil, errs.ExecSqlFailed
	}
	//从redis中查最新k线
	data, err := l.svcCtx.RedisClient.Hget(define.Kline.WithParams(), l.svcCtx.Config.SymbolInfo.SymbolName+"_"+in.KlineType.String())
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return &pb.GetKlineResp{KlineList: nil}, nil
		}
		logx.Errorw("get redis data failed", logger.ErrorField(err))
		return nil, err
	}
	var latestKline m.RedisModel
	if err := json.Unmarshal([]byte(data), &latestKline); err != nil {
		logx.Errorw("unmarshal redis data failed", logger.ErrorField(err))
		return nil, errs.RedisErr
	}

	switch {
	case latestKline.StartTime <= in.EntTime && len(klineList) == 300:
		copy(klineList[1:], klineList[:])
		klineList[0] = &latestKline.Kline
	case latestKline.StartTime <= in.EntTime && len(klineList) < 300:
		//数组整体后移
		klineList = append(klineList, &latestKline.Kline)
		copy(klineList[1:], klineList[:])
		klineList[0] = &latestKline.Kline

	}

	//考虑到给的参数的区间可能很大，所以从后往前推300
	klineResp := make([]*pb.GetKlineResp_Kline, 0, len(klineList))
	for i := len(klineList) - 1; i >= 0; i-- {
		d := &pb.GetKlineResp_Kline{
			StartTime: klineList[i].StartTime,
			EndTime:   klineList[i].EndTime,
			Symbol:    klineList[i].Symbol,
			Open:      utils.PrecCut(klineList[i].Open, quoteCoinPrec),
			High:      utils.PrecCut(klineList[i].High, quoteCoinPrec),
			Low:       utils.PrecCut(klineList[i].Low, quoteCoinPrec),
			Close:     utils.PrecCut(klineList[i].Close, quoteCoinPrec),
			Volume:    utils.PrecCut(klineList[i].Volume, quoteCoinPrec),
			Amount:    utils.PrecCut(klineList[i].Amount, baseCoinPrec),
			Range:     klineList[i].Range,
		}
		klineResp = append(klineResp, d)
	}

	return &pb.GetKlineResp{KlineList: klineResp}, nil
}
