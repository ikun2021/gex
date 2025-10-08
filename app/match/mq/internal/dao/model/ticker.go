package model

import (
	"github.com/ikun2021/gex/common/proto/define"
	"github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	"github.com/shopspring/decimal"
)

type Ticker struct {
	TimeUnix   int64           //最近的一笔的数据
	Volume     decimal.Decimal //成交额 计价币数量
	High       decimal.Decimal //最高价
	Low        decimal.Decimal //最低价
	Last24     decimal.Decimal //24小时之前
	Price      decimal.Decimal //当前
	Amount     decimal.Decimal //成交量 基础币数量
	Range      decimal.Decimal //变化百分比
	PriceDelta decimal.Decimal //变化数量
}

func (t *Ticker) CastToTickerRedisData(symbolInfo *define.SymbolInfo) TickerRedisData {
	return TickerRedisData{
		Volume:     t.Volume.String(),
		TimeUnix:   t.TimeUnix,
		High:       t.High.String(),
		Low:        t.Low.String(),
		Last24:     t.Last24.String(),
		Price:      t.Price.String(),
		Amount:     t.Amount.String(),
		PriceDelta: t.PriceDelta.String(),
		Symbol:     symbolInfo.SymbolName,
		Range:      t.Range.Mul(utils.NewFromStringMaxPrec("100")).StringFixedBank(3),
	}
}
func (t *Ticker) CastToTickerWsData(symbolInfo *define.SymbolInfo) ws.Ticker {
	return ws.Ticker{
		Price:           t.Price.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		High:            t.High.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		Low:             t.Low.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		Amount:          t.Amount.StringFixedBank(symbolInfo.BaseCoinPrec.Load()),
		Volume:          t.Volume.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		Range:           t.Range.Mul(utils.NewFromStringMaxPrec("100")).StringFixedBank(3),
		Last24HourPrice: t.Last24.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		Symbol:          symbolInfo.SymbolName,
	}
}

// TickerRedisData 用来存储的结构体
type TickerRedisData struct {
	Amount     string `json:"amount"`      //成交量
	TimeUnix   int64  `json:"time"`        //成交量
	High       string `json:"high"`        //最高价
	Low        string `json:"low"`         //最低价
	Last24     string `json:"last24price"` //24小时之前
	Price      string `json:"price"`       //当前
	Volume     string `json:"volume"`      //成交额
	Range      string `json:"range"`       //涨跌幅
	Symbol     string `json:"symbol"`
	PriceDelta string `json:"priceDelta"` //变化数量
}

func (t TickerRedisData) CastToTicker() Ticker {
	return Ticker{
		TimeUnix:   t.TimeUnix,
		Volume:     utils.NewFromStringMaxPrec(t.Volume),
		High:       utils.NewFromStringMaxPrec(t.High),
		Low:        utils.NewFromStringMaxPrec(t.Low),
		Last24:     utils.NewFromStringMaxPrec(t.Last24),
		Price:      utils.NewFromStringMaxPrec(t.Price),
		Amount:     utils.NewFromStringMaxPrec(t.Amount),
		Range:      utils.NewFromStringMaxPrec(t.Range),
		PriceDelta: utils.NewFromStringMaxPrec(t.PriceDelta),
	}
}
