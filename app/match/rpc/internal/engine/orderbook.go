package engine

import (
	"fmt"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	enum "github.com/ikun2021/gex/common/proto/enum"
	"github.com/shopspring/decimal"
)

type Key struct {
	price decimal.Decimal
	id    int64
}

// OrderBook 订单簿
type OrderBook struct {
	orderBook *rbt.Tree
	side      enum.Side
}

type DepthPosition struct {
	Price string `json:"Price"`
	Qty   string `json:"qty"`
}

func NewOrderBook(side enum.Side) *OrderBook {
	order := &OrderBook{
		side: side,
	}
	orderBook := rbt.NewWith(order.PriceComparator)
	order.orderBook = orderBook
	return order
}
func (ob *OrderBook) add(order *Order) {
	k := &Key{
		price: order.Price,
		id:    order.SequenceId,
	}
	//加入到订单簿中
	ob.orderBook.Put(k, order)

}
func (ob *OrderBook) remove(order *Order) {
	k := &Key{
		price: order.Price,
		id:    order.SequenceId,
	}
	ob.orderBook.Remove(k)
}

func (ob *OrderBook) PriceComparator(a, b interface{}) int {
	aAsserted := a.(*Key)
	bAsserted := b.(*Key)

	if result := aAsserted.price.Cmp(bAsserted.price); result != 0 {
		if ob.side == enum.Side_Buy {
			//卖盘从小到大
			//买盘的的话加一个负号，买盘从大到小。
			return -result
		}
		return result
	}
	switch {
	case aAsserted.id > bAsserted.id:
		return 1
	case aAsserted.id < bAsserted.id:
		return -1
	default:
		return 0
	}

}

func (ob *OrderBook) String() string {
	var str string
	values := ob.orderBook.Values()
	if ob.side == enum.Side_Sell {
		for i := len(values) - 1; i >= 0; i-- {
			order := values[i].(*Order)
			str += fmt.Sprintf("[side=%v]orderID=%v Price=%v qty=%v unfilledQty=%v Amount=%v unfilledAmount=%v\n", enum.Side_Sell, order.OrderID, order.Price, order.Qty, order.UnfilledQty, order.Amount, order.UnfilledAmount)

		}

	} else {
		for i := 0; i < len(values); i++ {
			order := values[i].(*Order)
			str += fmt.Sprintf("[side=%v]orderID=%v Price=%v qty=%v unfilledQty=%v Amount=%v unfilledAmount=%v\n", enum.Side_Buy, order.OrderID, order.Price, order.Qty, order.UnfilledQty, order.Amount, order.UnfilledAmount)
		}
	}
	return str
}
