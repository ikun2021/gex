package model

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quotes/kline/rpc/internal/dao/model"
	"github.com/ikun2021/gex/app/quotes/kline/rpc/pb"
	"github.com/ikun2021/gex/common/proto/define"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	"github.com/shopspring/decimal"
)

type StoreKline struct {
	Klines    []*Kline
	MessageID pulsar.MessageID
	IsHistory bool
	MatchID   int64
}

type Kline struct {
	KlineType   KlineType
	StartTime   int64           //周期的开始时间
	EndTime     int64           //周期的结束时间
	Amount      decimal.Decimal //成交额
	Volume      decimal.Decimal //成交量
	Open        decimal.Decimal //开盘价
	High        decimal.Decimal //高
	Low         decimal.Decimal //低
	Close       decimal.Decimal //收盘价
	Range       string          //涨跌幅
	InitMatchID int64
}

func (k *Kline) Copy() Kline {
	return Kline{
		StartTime: k.StartTime,
		EndTime:   k.EndTime,
		KlineType: k.KlineType,
		Open:      k.Open.Copy(),
		High:      k.High.Copy(),
		Low:       k.Low.Copy(),
		Close:     k.Close.Copy(),
		Volume:    k.Volume.Copy(),
		Amount:    k.Amount.Copy(),
		Range:     k.Range,
	}
}

func (k *Kline) CastToMysqlData(symbolInfo *define.SymbolInfo) *model.Kline {
	return &model.Kline{
		StartTime: k.StartTime,
		EndTime:   k.EndTime,
		Symbol:    symbolInfo.SymbolName,
		SymbolID:  symbolInfo.SymbolID,
		KlineType: int32(k.KlineType),
		Open:      k.Open.String(),
		High:      k.High.String(),
		Low:       k.Low.String(),
		Close:     k.Close.String(),
		Volume:    k.Volume.String(),
		Amount:    k.Amount.String(),
		Range:     k.Range,
	}
}
func (k *Kline) CastToRedisData(symbolInfo *define.SymbolInfo, matchID int64) *RedisModel {
	return &RedisModel{
		Kline: model.Kline{
			StartTime: k.StartTime,
			EndTime:   k.EndTime,
			Symbol:    symbolInfo.SymbolName,
			SymbolID:  symbolInfo.SymbolID,
			KlineType: int32(k.KlineType),
			Open:      k.Open.String(),
			High:      k.High.String(),
			Low:       k.Low.String(),
			Close:     k.Close.String(),
			Volume:    k.Volume.String(),
			Amount:    k.Amount.String(),
			Range:     k.Range,
		},
		MatchID: matchID,
	}
}

func (k *Kline) CastToWsData(symbolInfo *define.SymbolInfo) commonWs.Kline {
	return commonWs.Kline{
		StartTime: k.StartTime,
		EndTime:   k.EndTime,
		KlineType: int32(k.KlineType),
		Open:      utils.PrecCut(k.Open.String(), symbolInfo.QuoteCoinPrec.Load()),
		High:      utils.PrecCut(k.High.String(), symbolInfo.QuoteCoinPrec.Load()),
		Low:       utils.PrecCut(k.Low.String(), symbolInfo.QuoteCoinPrec.Load()),
		Close:     utils.PrecCut(k.Close.String(), symbolInfo.QuoteCoinPrec.Load()),
		Volume:    utils.PrecCut(k.Volume.String(), symbolInfo.QuoteCoinPrec.Load()),
		Amount:    utils.PrecCut(k.Amount.String(), symbolInfo.BaseCoinPrec.Load()),
		Range:     k.Range,
		Symbol:    symbolInfo.SymbolName,
	}
}

type KlineType pb.KlineType

const (
	Min1 KlineType = iota + 1
	Min5
	Min10
	Min15
	Min30
	Hour1
	Hour4
	Day1
	Week1
	Month1
)

var KlineTypes = []KlineType{
	Min1,
	Min5,
	Min10,
	Min15,
	Min30,
	Hour1,
	Hour4,
	Day1,
	Week1,
	Month1,
}

func (kt KlineType) String() string {
	return pb.KlineType(kt).String()
}
func (kt KlineType) GetCycle() int32 {
	switch kt {
	case Min1:
		return 60
	case Min5:
		return 300
	case Min10:
		return 600
	case Min15:
		return 900
	case Min30:
		return 1800
	case Hour1:
		return 3600
	case Hour4:
		return 14400
	case Day1:
		return 86400
	case Week1:
		return 604800
	case Month1:
		return 2678400
	default:
		return 0
	}
}
