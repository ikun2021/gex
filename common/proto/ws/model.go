package ws

import (
	"encoding/json"
	"strings"
)

type TopicPrefix string

const (
	TickerPrefix     TopicPrefix = "ticker"
	KlinePrefix      TopicPrefix = "kline"
	DepthPrefix      TopicPrefix = "depth"
	MiniTickerPrefix TopicPrefix = "miniTicker"
	TickPrefix       TopicPrefix = "tick"
	OrderPrefix      TopicPrefix = "order"
)

func (w TopicPrefix) WithParam(param ...string) string {
	if len(param) == 0 {
		return string(w)
	}
	return string(w) + "@" + strings.Join(param, "@")
}

// Kline k线信息
type Kline struct {
	KlineType int32  `json:"kt"`
	Open      string `json:"o"`
	High      string `json:"h"`
	Low       string `json:"l"`
	Close     string `json:"c"`
	Volume    string `json:"v"` //成立额
	Amount    string `json:"a"` //成交量
	StartTime int64  `json:"st"`
	EndTime   int64  `json:"et"`
	Range     string `json:"r"`
	Symbol    string `json:"s"`
}

// Ticker 24小时行情
type Ticker struct {
	Price           string `json:"lp"`
	High            string `json:"h"`
	Low             string `json:"l"`
	Amount          string `json:"a"`
	Volume          string `json:"v"`
	Range           string `json:"r"`
	Symbol          string `json:"s"`
	Last24HourPrice string `json:"l24p"`
}

// MiniTicker 精简24小时行情
type MiniTicker struct {
	LatestPrice string `json:"lp"`
	Range       string `json:"r"`
	Symbol      string `json:"s"`
}

// Depth 深度
type Depth struct {
	//上一个版本
	LastVersion string `json:"lv"`
	//当前版本
	CurrentVersion string `json:"cv"`
	//交易对
	Symbol string `json:"s"`
	//卖盘
	Asks [][]string `json:"a"`
	//买盘
	Bids [][]string `json:"b"`
}

// Tick 成交信息
type Tick struct {
	Price        string `json:"p"`
	BaseAmount   string `json:"b"`
	QuoteAmount  string `json:"q"`
	TimeStamp    int64  `json:"ts"`
	TakerIsBuyer bool   `json:"tib"`
}

// Order 最新订单状态
type Order struct {
	Id             string `json:"id"`
	OrderId        string `json:"oi"`
	SymbolName     string `json:"sn"`
	Price          string `json:"p"`
	Qty            string `json:"q"`
	Amount         string `json:"a"`
	Side           int8   `json:"si"`
	Status         int8   `json:"s"`
	OrderType      int8   `json:"ot"`
	FilledAmount   string `json:"fa"`
	FilledQty      string `json:"fq"`
	FilledAvgPrice string `json:"fap"`
	Uid            string `json:"u"`
	CreatedAt      int64  `json:"ca"`
}

type WsDataModel interface {
	Kline | Ticker | MiniTicker | Depth | Tick | Order
}
type Message[T WsDataModel] struct {
	Topic   string `json:"t"`
	Payload T      `json:"p"`
}

func (m Message[T]) ToBytes() []byte {
	data, _ := json.Marshal(m)
	return data
}
