package model

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shopspring/decimal"
)

type MatchData struct {
	MessageID  pulsar.MessageID `json:"_"`
	MatchTime  int64            //撮合时间
	MatchID    int64            //撮合ID
	Volume     decimal.Decimal  //成交额
	Amount     decimal.Decimal  //成交量
	StartPrice decimal.Decimal  //本次撮合开始的价格
	EndPrice   decimal.Decimal  //本次撮合结束的价格
	Low        decimal.Decimal  //本次撮合的最高价
	High       decimal.Decimal  //本次撮合的最低价
}
