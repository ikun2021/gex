package engine

import (
	"github.com/apache/pulsar-client-go/pulsar"
	enum "github.com/ikun2021/gex/common/proto/enum"
	"github.com/shopspring/decimal"
)

// Order 订单
type Order struct {
	MessageId           int64            `json:"message_id"`
	PulsarMsgId         pulsar.MessageID `json:"-"`
	OrderID             string           `json:"order_id"`
	OrderPkId           int64            `json:"order_pk_id"`
	CreateTime          int64            `json:"create_time"`
	IsCancel            bool             `json:"is_cancel"`
	Uid                 int64            `json:"uid"`                   //用户id
	Price               decimal.Decimal  `json:"price"`                 //价格
	BaseAmount          decimal.Decimal  `json:"base_amount"`           //数量 市价单位零
	OrderType           enum.OrderType   `json:"order_type"`            //订单类型 市价单 限价单
	QuoteAmount         decimal.Decimal  `json:"quote_amount"`          //金额
	Side                enum.Side        `json:"side"`                  //方向
	OrderStatus         enum.OrderStatus `json:"order_status"`          //订单状态
	UnfilledBaseAmount  decimal.Decimal  `json:"unfilled_base_amount"`  //未成交数量
	FilledBaseAmount    decimal.Decimal  `json:"filled_base_amount"`    //已成交数量
	UnfilledQuoteAmount decimal.Decimal  `json:"unfilled_quote_amount"` //未成交金额
	FilledQuoteAmount   decimal.Decimal  `json:"filled_quote_amount"`   //成交金额
}
