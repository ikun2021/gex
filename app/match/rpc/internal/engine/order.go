package engine

import (
	enum "github.com/ikun2021/gex/common/proto/enum"
	"github.com/shopspring/decimal"
)

// Order 订单
type Order struct {
	OrderID        string
	SequenceId     int64
	CreateTime     int64
	IsCancel       bool
	Uid            int64            //用户id
	Price          decimal.Decimal  //价格
	Qty            decimal.Decimal  //数量 市价单位零
	OrderType      enum.OrderType   //订单类型 市价单 限价单
	Amount         decimal.Decimal  //金额
	Side           enum.Side        //方向
	OrderStatus    enum.OrderStatus //订单状态
	UnfilledQty    decimal.Decimal  //未成交数量
	FilledQty      decimal.Decimal  //已成交数量
	UnfilledAmount decimal.Decimal  //未成交金额
	FilledAmount   decimal.Decimal  //成交金额
}
