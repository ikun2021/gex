package logic

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ikun2021/gex/app/match/rpc/internal/dao/model"
	"github.com/ikun2021/gex/app/match/rpc/internal/svc"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"

	"github.com/ikun2021/gex/app/match/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
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

// 获取ticker
func (l *GetTickerLogic) GetTicker(in *pb.GetTickerReq) (*pb.GetTickerResp, error) {
	quoteCoinPrec := l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()
	baseCoinPrec := l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()
	resp := &pb.GetTickerResp{}
	if in.Symbol == "" {
		data, err := l.svcCtx.RedisClient.Hgetall(string(define.Ticker))
		respData := make([]*pb.GetTickerResp_Ticker, 0, len(data))
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return resp, nil
			}
			logx.Errorw("query from redis failed", logger.ErrorField(err))
			return nil, errs.RedisErr
		}
		for _, v := range data {
			var tickerRedisData model.TickerRedisData
			if err := json.Unmarshal([]byte(v), &tickerRedisData); err != nil {
				return nil, errs.Internal
			}
			d := &pb.GetTickerResp_Ticker{
				LatestPrice: utils.PrecCut(tickerRedisData.Price, quoteCoinPrec),
				High:        utils.PrecCut(tickerRedisData.High, quoteCoinPrec),
				Low:         utils.PrecCut(tickerRedisData.Low, quoteCoinPrec),
				Volume:      utils.PrecCut(tickerRedisData.Volume, quoteCoinPrec),
				Amount:      utils.PrecCut(tickerRedisData.Amount, baseCoinPrec),
				Last24Price: utils.PrecCut(tickerRedisData.Last24, quoteCoinPrec),
				PriceRange:  tickerRedisData.Range,
				Symbol:      tickerRedisData.Symbol,
			}
			respData = append(respData, d)
		}
		resp.TickerList = respData
	} else {
		respData := make([]*pb.GetTickerResp_Ticker, 0, 10)

		var tickerRedisData model.TickerRedisData
		data, err := l.svcCtx.RedisClient.Hget(string(define.Ticker), in.Symbol)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				d := &pb.GetTickerResp_Ticker{
					LatestPrice: "0",
					High:        "0",
					Low:         "0",
					Volume:      "0",
					Amount:      "0",
					Last24Price: "0",
					PriceRange:  "0",
					Symbol:      tickerRedisData.Symbol,
				}
				respData = append(respData, d)
				resp.TickerList = respData
				return resp, nil
			}
			logx.Errorw("query from redis failed", logger.ErrorField(err))
			return nil, errs.RedisErr
		}
		if err := json.Unmarshal([]byte(data), &tickerRedisData); err != nil {
			logx.Errorw("unmarshal from redis failed", logger.ErrorField(err))
			return nil, errs.Internal
		}
		d := &pb.GetTickerResp_Ticker{
			LatestPrice: utils.PrecCut(tickerRedisData.Price, quoteCoinPrec),
			High:        utils.PrecCut(tickerRedisData.High, quoteCoinPrec),
			Low:         utils.PrecCut(tickerRedisData.Low, quoteCoinPrec),
			Volume:      utils.PrecCut(tickerRedisData.Volume, quoteCoinPrec),
			Amount:      utils.PrecCut(tickerRedisData.Amount, baseCoinPrec),
			Last24Price: utils.PrecCut(tickerRedisData.Last24, quoteCoinPrec),
			PriceRange:  tickerRedisData.Range,
			Symbol:      tickerRedisData.Symbol,
		}
		respData = append(respData, d)
		resp.TickerList = respData
	}

	return resp, nil
}
