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

func (ob *OrderBook) Copy() *OrderBook {
	newOrderBook := NewOrderBook(ob.side)

	// 复制所有订单
	values := ob.orderBook.Values()
	for _, value := range values {
		order := value.(*InputMessage)
		// 创建新的订单副本
		newOrder := *order // 复制订单结构体
		newOrderBook.add(&newOrder)
	}

	return newOrderBook
}

func (ob *OrderBook) dump() {
	sideStr := "买盘 (BIDS)"
	if ob.side == enum.Side_Sell {
		sideStr = "卖盘 (ASKS)"
	}
	fmt.Printf("📋 ########盘口(%s): #######\n", sideStr)
	fmt.Printf("📊 订单总数: %d\n", ob.orderBook.Size())
	fmt.Printf("📈 方向: %s\n", func() string {
		switch ob.side {
		case enum.Side_Buy:
			return "买入 (BUY)"
		case enum.Side_Sell:
			return "卖出 (SELL)"
		default:
			return "未知 (UNKNOWN)"
		}
	}())

	fmt.Println("📝 订单列表:")

	values := ob.orderBook.Values()
	if ob.side == enum.Side_Sell {
		// 卖盘按价格从高到低显示
		for i := len(values) - 1; i >= 0; i-- {
			order := values[i].(*InputMessage)
			fmt.Printf("  📄 订单 #%d:\n", i+1)
			fmt.Printf("    🆔 订单ID: %s\n", order.OrderID)
			fmt.Printf("    📍 价格: %s\n", order.Price.String())
			fmt.Printf("    📦 总数量: %s\n", order.BaseAmount.String())
			fmt.Printf("    📥 已成交数量: %s\n", order.FilledBaseAmount.String())
			fmt.Printf("    📤 未成交数量: %s\n", order.UnfilledBaseAmount.String())
			fmt.Printf("    💰 总金额: %s\n", order.QuoteAmount.String())
			fmt.Printf("    💳 已成交金额: %s\n", order.FilledQuoteAmount.String())
			fmt.Printf("    💸 未成交金额: %s\n", order.UnfilledQuoteAmount.String())
			fmt.Printf("    👤 用户ID: %d\n", order.Uid)
			fmt.Printf("    📊 订单状态: %s\n", order.OrderStatus.String())
		}
	} else {
		// 买盘按价格从高到低显示
		for i := len(values) - 1; i >= 0; i-- {
			order := values[i].(*InputMessage)
			fmt.Printf("  📄 订单 #%d:\n", i+1)
			fmt.Printf("    🆔 订单ID: %s\n", order.OrderID)
			fmt.Printf("    📍 价格: %s\n", order.Price.String())
			fmt.Printf("    📦 总数量: %s\n", order.BaseAmount.String())
			fmt.Printf("    📥 已成交数量: %s\n", order.FilledBaseAmount.String())
			fmt.Printf("    📤 未成交数量: %s\n", order.UnfilledBaseAmount.String())
			fmt.Printf("    💰 总金额: %s\n", order.QuoteAmount.String())
			fmt.Printf("    💳 已成交金额: %s\n", order.FilledQuoteAmount.String())
			fmt.Printf("    💸 未成交金额: %s\n", order.UnfilledQuoteAmount.String())
			fmt.Printf("    👤 用户ID: %d\n", order.Uid)
			fmt.Printf("    📊 订单状态: %s\n", order.OrderStatus.String())
		}
	}

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
func (ob *OrderBook) add(order *InputMessage) {
	k := &Key{
		price: order.Price,
		id:    order.OrderPkId,
	}
	//加入到订单簿中
	ob.orderBook.Put(k, order)

}
func (ob *OrderBook) remove(order *InputMessage) {
	k := &Key{
		price: order.Price,
		id:    order.OrderPkId,
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
			order := values[i].(*InputMessage)
			str += fmt.Sprintf("[side=%v]orderID=%v Price=%v qty=%v unfilledQty=%v QuoteAmount=%v unfilledAmount=%v\n", enum.Side_Sell, order.OrderID, order.Price, order.BaseAmount, order.UnfilledBaseAmount, order.QuoteAmount, order.UnfilledQuoteAmount)

		}

	} else {
		for i := 0; i < len(values); i++ {
			order := values[i].(*InputMessage)
			str += fmt.Sprintf("[side=%v]orderID=%v Price=%v qty=%v unfilledQty=%v QuoteAmount=%v unfilledAmount=%v\n", enum.Side_Buy, order.OrderID, order.Price, order.BaseAmount, order.UnfilledBaseAmount, order.QuoteAmount, order.UnfilledQuoteAmount)
		}
	}
	return str
}
