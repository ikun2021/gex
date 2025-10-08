package model

import (
	"github.com/ikun2021/gex/common/proto/define"
	"github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	"github.com/shopspring/decimal"
)

type Ticker struct {
	TimeUnix   int64           //最近的一笔的数据
	amount     decimal.Decimal //成交额 计价币数量
	High       decimal.Decimal //最高价
	Low        decimal.Decimal //最低价
	Last24     decimal.Decimal //24小时之前
	Price      decimal.Decimal //当前
	Volume     decimal.Decimal //成交量 基础币数量
	Range      decimal.Decimal //变化百分比
	PriceDelta decimal.Decimal //变化数量
}

func (t *Ticker) CastToTickerWsData(symbolInfo define.SymbolInfo) ws.Ticker {
	return ws.Ticker{
		Price:  t.Last24.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		High:   t.High.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		Low:    t.Low.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		Amount: t.Volume.StringFixedBank(symbolInfo.BaseCoinPrec.Load()),
		Volume: t.amount.StringFixedBank(symbolInfo.QuoteCoinPrec.Load()),
		Range:  t.Range.StringFixedBank(3),
		Symbol: symbolInfo.SymbolName,
	}
}

// TickerRedisData 用来存储的结构体
type TickerRedisData struct {
	Volume     string `json:"volume"`      //成交量
	TimeUnix   int64  `json:"time"`        //成交量
	High       string `json:"high"`        //最高价
	Low        string `json:"low"`         //最低价
	Last24     string `json:"last24price"` //24小时之前
	Price      string `json:"price"`       //当前
	Amount     string `json:"amount"`      //成交额
	Range      string `json:"range"`       //涨跌幅
	Symbol     string `json:"symbol"`
	PriceDelta string `json:"priceDelta"` //变化数量
}

func (t TickerRedisData) CastToTicker() Ticker {
	return Ticker{
		TimeUnix:   t.TimeUnix,
		amount:     utils.NewFromStringMaxPrec(t.Amount),
		High:       utils.NewFromStringMaxPrec(t.High),
		Low:        utils.NewFromStringMaxPrec(t.Low),
		Last24:     utils.NewFromStringMaxPrec(t.Last24),
		Price:      utils.NewFromStringMaxPrec(t.Price),
		Volume:     utils.NewFromStringMaxPrec(t.Volume),
		Range:      utils.NewFromStringMaxPrec(t.Range),
		PriceDelta: utils.NewFromStringMaxPrec(t.PriceDelta),
	}
}
