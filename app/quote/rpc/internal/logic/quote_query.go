package logic

import (
	"encoding/json"
	"errors"

	"github.com/ikun2021/gex/app/quote/rpc/internal/dao"
	"github.com/ikun2021/gex/app/quote/rpc/pb"
	"github.com/ikun2021/gex/common/proto/define"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

func historyToPbKline(d *dao.KlineHistory) *pb.GetKlineResp_Kline {
	if d == nil {
		return nil
	}
	return &pb.GetKlineResp_Kline{
		Open:      d.Open,
		High:      d.High,
		Low:       d.Low,
		Close:     d.Close,
		Volume:    d.Volume,
		Amount:    d.Amount,
		StartTime: d.StartTime,
		EndTime:   d.EndTime,
		Range:     d.Range,
		Symbol:    d.Symbol,
	}
}

func redisModelToPbKline(d *dao.RedisModel) *pb.GetKlineResp_Kline {
	if d == nil {
		return nil
	}
	return &pb.GetKlineResp_Kline{
		Open:      d.Open,
		High:      d.High,
		Low:       d.Low,
		Close:     d.Close,
		Volume:    d.Volume,
		Amount:    d.Amount,
		StartTime: d.StartTime,
		EndTime:   d.EndTime,
		Range:     d.Range,
		Symbol:    d.Symbol,
	}
}

func wsTickerToPb(t commonWs.Ticker) *pb.Ticker {
	return &pb.Ticker{
		Open:   t.Last24HourPrice,
		High:   t.High,
		Low:    t.Low,
		Close:  t.Price,
		Volume: t.Volume,
		Amount: t.Amount,
		Range:  t.Range,
		Symbol: t.Symbol,
	}
}

func wsTickToPb(t commonWs.Tick, symbol string) *pb.Tick {
	side := int32(2)
	if t.TakerIsBuyer {
		side = 1
	}
	return &pb.Tick{
		Price:        t.Price,
		BaseAmount:   t.BaseAmount,
		QuoteAmount:  t.QuoteAmount,
		Side:         side,
		CreatedAt:    t.TimeStamp,
		TakerIsBuyer: t.TakerIsBuyer,
		Symbol:       symbol,
	}
}

func tickDocToPb(d *dao.TickDoc) *pb.Tick {
	if d == nil {
		return nil
	}
	return &pb.Tick{
		Price:        d.Price,
		BaseAmount:   d.BaseAmount,
		QuoteAmount:  d.QuoteAmount,
		Side:         d.Side,
		CreatedAt:    d.CreatedAt,
		TakerIsBuyer: d.Side == 1,
		Symbol:       d.Symbol,
	}
}

func loadLatestKlineFromRedis(client *redis.Redis, symbol string, klineType pb.KlineType) (*pb.GetKlineResp_Kline, error) {
	field := symbol + "_" + klineType.String()
	data, err := client.Hget(define.Kline.WithParams(), field)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var m dao.RedisModel
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return nil, err
	}
	return redisModelToPbKline(&m), nil
}

func mergeLatestKline(list []*pb.GetKlineResp_Kline, latest *pb.GetKlineResp_Kline) []*pb.GetKlineResp_Kline {
	if latest == nil {
		return list
	}
	for _, k := range list {
		if k.StartTime == latest.StartTime {
			return list
		}
	}
	return append(list, latest)
}
