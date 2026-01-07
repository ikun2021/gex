package engine

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/rpc/internal/config"
	enum "github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	ws "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stringx"
	"google.golang.org/protobuf/proto"
	"log"
	"math"
	"time"
)

// MatchEngine 撮合引擎
type MatchEngine struct {
	asks             *OrderBook      //卖盘
	bids             *OrderBook      //买盘
	bestBid          decimal.Decimal //买一价
	bestAsk          decimal.Decimal //卖一价
	baseCoinMinUnit  decimal.Decimal
	quoteCoinMinUnit decimal.Decimal
	depthHandler     *DepthHandler
	c                *config.Config
	producer         pulsar.Producer
	proxyClient      ws.ProxyClient
	//tick             chan *MatchResult
	currentSeqId int64
}

// MatchedRecord  一次撮合匹配的结果,一次撮合会多次匹配
type MatchedRecord struct {
	Price           decimal.Decimal
	Qty             decimal.Decimal
	Amount          decimal.Decimal
	MatchedRecordID string
	//最新的taker订单的状态
	Taker Order
	//最新的maker订单的状态
	Maker Order
}
type MatchResult struct {
	//每一次匹配的结构
	MatchedRecords []*MatchedRecord
	//本次撮合的id
	MatchID    string
	CancelResp *CancelResp
	//撮合时间
	MatchTime int64
	//taker为买单
	TakerIsBuy bool
}
type CancelResp struct {
	//取消订单的id，如果不为空则表示取消订单。
	CancelId int64
	//币种id 取消买单则为计价币id，取消卖单则为基础币id
	CoinId int32
	//数量 取消买单则为计价币数量，取消卖单则为基础币数量
	Amount string
	//用户id
	Uid int64
}

func (mr *MatchResult) String() {
	fmt.Printf("============================================================\n")
	fmt.Printf("matchID=%v cancelOrderID=%v\n", mr.MatchID, mr.CancelResp)
	for _, v := range mr.MatchedRecords {
		fmt.Printf("matchID=%v matchedRecord Price=%v qty=%v QuoteAmount=%v\n ", v.MatchedRecordID, v.Price, v.Qty, v.Amount)
		fmt.Printf("taker=%+v\n", v.Taker)
		fmt.Printf("maker=%+v\n", v.Maker)
	}
	fmt.Printf("============================================================\n")

}

func NewMatchEngine(c *config.Config, producer pulsar.Producer, proxyClient ws.ProxyClient) *MatchEngine {
	me := &MatchEngine{
		asks:         NewOrderBook(enum.Side_Sell),
		bids:         NewOrderBook(enum.Side_Buy),
		bestBid:      utils.DecimalZeroMaxPrec,
		bestAsk:      utils.DecimalZeroMaxPrec,
		depthHandler: NewDepthHandler(0, c, proxyClient),
		c:            c,
		producer:     producer,
		proxyClient:  proxyClient,
	}
	return me
}

func (m *MatchEngine) addOrder(order *Order) {
	if order.Side == enum.Side_Buy {
		m.bids.add(order)
		m.updateBestBid()
	} else {
		m.asks.add(order)
		m.updateBestAsk()
	}

}
func (m *MatchEngine) cancelOrder(order *Order) {
	if order.Side == enum.Side_Buy {
		m.bids.remove(order)
		m.updateBestBid()
	} else {
		m.asks.remove(order)
		m.updateBestAsk()
	}
}

// 更新买一价
func (m *MatchEngine) updateBestBid() {
	if m.bids.orderBook.Size() == 0 {
		m.bestBid = utils.DecimalZeroMaxPrec
	} else {
		m.bestBid = m.bids.orderBook.Left().Key.(*Key).price
	}
}

// 更新卖一价
func (m *MatchEngine) updateBestAsk() {
	if m.asks.orderBook.Size() == 0 {
		m.bestAsk = utils.DecimalZeroMaxPrec
	} else {
		m.bestAsk = m.asks.orderBook.Left().Key.(*Key).price
	}
}

// 匹配市价单卖单
func (m *MatchEngine) matchMarketOrderSell(takerOrder *Order) {

	matchedResult := &MatchResult{
		MatchedRecords: make([]*MatchedRecord, 0, 2),
		TakerIsBuy:     false,
	}
	//如果没有买盘，直接取消订单
	if m.bids.orderBook.Size() == 0 {
		matchedResult.CancelResp = &CancelResp{
			CancelId: takerOrder.SequenceId,
			CoinId:   m.c.SymbolInfo.BaseCoinID,
			Amount:   takerOrder.UnfilledBaseAmount.String(),
			Uid:      takerOrder.Uid,
		}
		m.SendMatchResult(matchedResult)
		return
	}

	iterator := m.bids.orderBook.Iterator()
	var matchedRecord *MatchedRecord
	deletedKeys := make([]*Key, 0, 2)
	for iterator.Next() {
		makerOrder := iterator.Value().(*Order)
		result := takerOrder.UnfilledBaseAmount.Cmp(makerOrder.UnfilledBaseAmount)
		switch {
		case result == 1:
			takerOrder.OrderStatus = enum.OrderStatus_PartFilled
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			takerOrder.UnfilledBaseAmount = takerOrder.UnfilledBaseAmount.Sub(qty)
			//takerOrder.UnfilledQuoteAmount = takerOrder.UnfilledQuoteAmount.Sub(amount)
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			takerOrder.FilledBaseAmount = takerOrder.FilledBaseAmount.Add(qty)
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			matchedRecord = &MatchedRecord{
				//订单的剩余数量
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))
		case result == 0:
			takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled

			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			takerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			takerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			takerOrder.FilledBaseAmount = takerOrder.FilledBaseAmount.Add(qty)

			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			matchedRecord = &MatchedRecord{

				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))
		case result == -1:
			takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			makerOrder.OrderStatus = enum.OrderStatus_PartFilled

			//更新订单的剩余数量
			qty := takerOrder.UnfilledBaseAmount
			//	amount := takerOrder.UnfilledQuoteAmount
			a := qty.Mul(makerOrder.Price)
			takerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			takerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledBaseAmount = makerOrder.UnfilledBaseAmount.Sub(qty)
			makerOrder.UnfilledQuoteAmount = makerOrder.UnfilledQuoteAmount.Sub(a)
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(a)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(a)
			takerOrder.FilledBaseAmount = takerOrder.FilledBaseAmount.Add(qty)

			//订单的剩余数量
			matchedRecord = &MatchedRecord{

				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: a,
			}
		}
		matchedRecord.Taker = *takerOrder
		matchedRecord.Maker = *makerOrder
		matchedRecord.MatchedRecordID = cast.ToString(idgen.NextId())
		matchedResult.MatchedRecords = append(matchedResult.MatchedRecords, matchedRecord)
		//订单全部成交退出，或者小于下一个订单的价格。不再循环匹配。
		if takerOrder.OrderStatus == enum.OrderStatus_ALLFilled {
			break
		}

	}
	//删除买盘被匹配过的订单，更新买一价
	if len(deletedKeys) > 0 {
		for _, v := range deletedKeys {
			m.bids.orderBook.Remove(v)
		}
		m.updateBestBid()
	}
	//更新深度数据
	for _, record := range matchedResult.MatchedRecords {
		p := &position{
			price: record.Price,
			qty:   record.Qty,
		}
		m.depthHandler.updateDepth(p, enum.Side_Buy, Delete, m.currentSeqId)
	}
	matchedResult.MatchTime = time.Now().UnixNano()
	matchedResult.MatchID = cast.ToString(idgen.NextId())
	m.SendMatchResult(matchedResult)

	if takerOrder.OrderStatus != enum.OrderStatus_ALLFilled {
		r := &MatchResult{
			CancelResp: &CancelResp{
				CancelId: takerOrder.SequenceId,
				CoinId:   m.c.SymbolInfo.BaseCoinID,
				Amount:   takerOrder.UnfilledBaseAmount.String(),
				Uid:      takerOrder.Uid,
			},
		}
		//发送取消订单
		m.SendMatchResult(r)

	}
}

// 市价买单 按照指定计价币的数量来买
func (m *MatchEngine) matchMarkerOrderBuy(takerOrder *Order) {

	matchedResult := &MatchResult{
		MatchedRecords: make([]*MatchedRecord, 0, 2),
		TakerIsBuy:     true,
	}
	//如果没有卖盘，直接取消订单
	if m.asks.orderBook.Size() == 0 {
		matchedResult.CancelResp = &CancelResp{
			CancelId: takerOrder.SequenceId,
			CoinId:   m.c.SymbolInfo.QuoteCoinID,
			Amount:   takerOrder.UnfilledQuoteAmount.String(),
			Uid:      takerOrder.Uid,
		}
		//m.Next <- matchedResult
		m.SendMatchResult(matchedResult)
		return
	}

	iterator := m.asks.orderBook.Iterator()
	//待被删除的key
	deletedKeys := make([]*Key, 0, 2)
	var matchedRecord *MatchedRecord
LOOP:
	for iterator.Next() {
		makerOrder := iterator.Value().(*Order)
		result := takerOrder.UnfilledQuoteAmount.Cmp(makerOrder.UnfilledQuoteAmount)
		switch result {
		case 1:
			takerOrder.OrderStatus = enum.OrderStatus_PartFilled
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			takerOrder.FilledBaseAmount = takerOrder.FilledBaseAmount.Add(qty)
			takerOrder.UnfilledQuoteAmount = takerOrder.UnfilledQuoteAmount.Sub(amount)
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			matchedRecord = &MatchedRecord{
				//订单的剩余数量
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))
		case 0:
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			takerOrder.FilledBaseAmount = takerOrder.FilledBaseAmount.Add(qty)
			takerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			matchedRecord = &MatchedRecord{
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))

		case -1:
			//taker金额比maker的金额要小，匹配结束
			//按照taker的金额购买的话能买多少，小于最小的单位则结束。
			qty := takerOrder.UnfilledQuoteAmount.Div(makerOrder.Price)
			baseCoinMinUnit := utils.NewFromStringMaxPrec(cast.ToString(math.Pow10(int(-m.c.SymbolInfo.BaseCoinPrec.Load()))))
			if qty.LessThan(baseCoinMinUnit) {
				break LOOP
			}
			makerOrder.OrderStatus = enum.OrderStatus_PartFilled
			takerOrder.OrderStatus = enum.OrderStatus_PartFilled
			//去除余数
			//数量
			q := qty.Div(baseCoinMinUnit).Floor().Mul(baseCoinMinUnit)
			//金额
			a := q.Mul(makerOrder.Price)
			//更新订单的剩余数量
			takerOrder.FilledBaseAmount = takerOrder.FilledBaseAmount.Add(q)
			takerOrder.UnfilledQuoteAmount = takerOrder.UnfilledQuoteAmount.Sub(a)
			makerOrder.UnfilledBaseAmount = makerOrder.UnfilledBaseAmount.Sub(q)
			makerOrder.UnfilledQuoteAmount = makerOrder.UnfilledQuoteAmount.Sub(a)
			if takerOrder.UnfilledQuoteAmount.Equal(utils.DecimalZeroMaxPrec) {
				takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			}
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(a)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(a)
			matchedRecord = &MatchedRecord{

				Price:  makerOrder.Price,
				Qty:    q,
				Amount: a,
			}
		}
		matchedRecord.Taker = *takerOrder
		matchedRecord.Maker = *makerOrder
		matchedRecord.MatchedRecordID = cast.ToString(idgen.NextId())
		matchedResult.MatchedRecords = append(matchedResult.MatchedRecords, matchedRecord)
	}
	matchedResult.MatchID = cast.ToString(idgen.NextId())
	//删除买盘中的被匹配完的订单，同时更新卖一价
	if len(deletedKeys) > 0 {
		for _, v := range deletedKeys {
			m.asks.orderBook.Remove(v)
		}
		m.updateBestAsk()
	}
	//更新深度数据
	for _, record := range matchedResult.MatchedRecords {
		p := &position{
			price: record.Price,
			qty:   record.Qty,
		}
		m.depthHandler.updateDepth(p, enum.Side_Sell, Delete, m.currentSeqId)
	}
	matchedResult.MatchTime = time.Now().UnixNano()
	//m.Next <- matchedResult
	if len(matchedResult.MatchedRecords) > 0 {
		m.SendMatchResult(matchedResult)
	}

	if takerOrder.OrderStatus != enum.OrderStatus_ALLFilled {
		r := &MatchResult{
			CancelResp: &CancelResp{
				CancelId: takerOrder.SequenceId,
				CoinId:   m.c.SymbolInfo.QuoteCoinID,
				Amount:   takerOrder.UnfilledQuoteAmount.String(),
				Uid:      takerOrder.Uid,
			},
		}
		//发送取消订单
		m.SendMatchResult(r)

	}
}

// 匹配限价买单
func (m *MatchEngine) matchLimitOrderBuy(takerOrder *Order) {
	matchedResult := &MatchResult{
		MatchedRecords: make([]*MatchedRecord, 0, 2),
		TakerIsBuy:     true,
	}
	//买单从卖盘中找
	iterator := m.asks.orderBook.Iterator()
	//待被删除的key
	deletedKeys := make([]*Key, 0, 2)
	for iterator.Next() {
		makerOrder := iterator.Value().(*Order)

		//订单全部成交退出，或者小于下一个订单的价格。不再循环匹配。
		if takerOrder.OrderStatus == enum.OrderStatus_ALLFilled || makerOrder.Price.GreaterThan(takerOrder.Price) {
			break
		}
		//计较价格
		result := takerOrder.UnfilledBaseAmount.Cmp(makerOrder.UnfilledBaseAmount)
		var matchedRecord *MatchedRecord
		switch {
		//吃完maker
		case result == 1:
			takerOrder.OrderStatus = enum.OrderStatus_PartFilled
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			//taker减的金额不能以maker为准，要以taker下单的价格乘以数量为准
			//比如 maker卖 price222 qty1 taker 买 price333 qty 1  本次匹配taker扣除的金额为333 成交的金额是已maker 222 为准
			takerAmount := qty.Mul(takerOrder.Price)
			takerOrder.UnfilledBaseAmount = takerOrder.UnfilledBaseAmount.Sub(qty)
			takerOrder.UnfilledQuoteAmount = takerOrder.UnfilledQuoteAmount.Sub(takerAmount)
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			matchedRecord = &MatchedRecord{
				//订单的剩余数量
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))

		case result == 0:
			takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled

			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			takerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			takerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			matchedRecord = &MatchedRecord{
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))
		case result == -1:
			takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			makerOrder.OrderStatus = enum.OrderStatus_PartFilled
			//更新订单的剩余数量
			qty := takerOrder.UnfilledBaseAmount
			//amount := takerOrder.UnfilledQuoteAmount
			takerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			takerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledBaseAmount = makerOrder.UnfilledBaseAmount.Sub(qty)
			makerAmount := makerOrder.Price.Mul(qty)
			//成交的金额不能使用taker的金额,使用maker成交的数量乘以maker的价格 比如 maker卖 price222 qty2 taker 买 price333 qty 1
			//maker的未成交金额 减 222 *1 价格以maker为准
			//taker buy 111 1 maker sell 100 1
			makerOrder.UnfilledQuoteAmount = makerOrder.UnfilledQuoteAmount.Sub(makerAmount)
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(makerAmount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(makerAmount)
			matchedRecord = &MatchedRecord{
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: makerAmount,
			}
		}
		//加入到匹配的结果中
		matchedRecord.Taker = *takerOrder
		matchedRecord.Maker = *makerOrder
		matchedRecord.MatchedRecordID = cast.ToString(idgen.NextId())
		matchedResult.MatchedRecords = append(matchedResult.MatchedRecords, matchedRecord)

	}
	//删除卖盘被匹配过的订单，更新卖一价
	if len(deletedKeys) > 0 {
		for _, v := range deletedKeys {
			m.asks.orderBook.Remove(v)
		}
		m.updateBestAsk()
	}
	//如果taker还是部分匹配，将订单加入的买盘中
	if takerOrder.OrderStatus == enum.OrderStatus_PartFilled {
		m.addOrder(takerOrder)
		p := &position{
			price: takerOrder.Price,
			qty:   takerOrder.UnfilledBaseAmount,
		}
		m.depthHandler.updateDepth(p, enum.Side_Buy, Add, m.currentSeqId)
	}
	//更新深度数据
	for _, record := range matchedResult.MatchedRecords {
		p := &position{
			price: record.Price,
			qty:   record.Qty,
		}
		m.depthHandler.updateDepth(p, enum.Side_Sell, Delete, m.currentSeqId)

	}
	matchedResult.MatchTime = time.Now().UnixNano()
	matchedResult.MatchID = cast.ToString(idgen.NextId())
	//发送撮合结果
	m.SendMatchResult(matchedResult)

}

// 匹配限价卖单
func (m *MatchEngine) matchLimitOrderSell(takerOrder *Order) {
	matchedResult := &MatchResult{
		MatchedRecords: make([]*MatchedRecord, 0, 2),
		TakerIsBuy:     false,
	}
	//遍历买盘
	iterator := m.bids.orderBook.Iterator()
	var matchedRecord *MatchedRecord
	deletedKeys := make([]*Key, 0, 2)
	for iterator.Next() {
		makerOrder := iterator.Value().(*Order)
		//订单全部成交退出，或者小于下一个订单的价格。不再循环匹配。
		if takerOrder.OrderStatus == enum.OrderStatus_ALLFilled || takerOrder.Price.GreaterThan(makerOrder.Price) {
			break
		}

		result := takerOrder.UnfilledBaseAmount.Cmp(makerOrder.UnfilledBaseAmount)
		switch {
		case result == 1:
			takerOrder.OrderStatus = enum.OrderStatus_PartFilled
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			//taker减的金额不能以maker为准，要以taker下单的价格乘以数量为准 比如 maker买 price444 qty1 taker 卖 price333 qty 2 本次匹配taker扣除的金额为333
			takerAmount := qty.Mul(takerOrder.Price)
			takerOrder.UnfilledBaseAmount = takerOrder.UnfilledBaseAmount.Sub(qty)
			takerOrder.UnfilledQuoteAmount = takerOrder.UnfilledQuoteAmount.Sub(takerAmount)
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec

			makerOrder.FilledBaseAmount = qty
			takerOrder.FilledBaseAmount = qty
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			matchedRecord = &MatchedRecord{
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))
		case result == 0:
			takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			makerOrder.OrderStatus = enum.OrderStatus_ALLFilled

			//更新订单的剩余数量
			qty := makerOrder.UnfilledBaseAmount
			amount := makerOrder.UnfilledQuoteAmount
			takerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			takerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(amount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(amount)
			//加入到撮合记录
			//卖单吃买单，以买单价格为准
			matchedRecord = &MatchedRecord{
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: amount,
			}
			//将key加入的集合中
			deletedKeys = append(deletedKeys, iterator.Key().(*Key))
		case result == -1:
			takerOrder.OrderStatus = enum.OrderStatus_ALLFilled
			makerOrder.OrderStatus = enum.OrderStatus_PartFilled

			//更新订单的剩余数量
			qty := takerOrder.UnfilledBaseAmount
			//amount := takerOrder.UnfilledQuoteAmount
			takerOrder.UnfilledBaseAmount = utils.DecimalZeroMaxPrec
			takerOrder.UnfilledQuoteAmount = utils.DecimalZeroMaxPrec
			makerOrder.UnfilledBaseAmount = makerOrder.UnfilledBaseAmount.Sub(qty)
			makerAmount := makerOrder.Price.Mul(qty)
			//成交的金额不能使用taker的金额
			//使用maker成交的数量乘以maker的价格
			makerOrder.UnfilledQuoteAmount = makerOrder.UnfilledQuoteAmount.Sub(makerAmount)
			takerOrder.FilledQuoteAmount = takerOrder.FilledQuoteAmount.Add(makerAmount)
			makerOrder.FilledQuoteAmount = makerOrder.FilledQuoteAmount.Add(makerAmount)
			matchedRecord = &MatchedRecord{
				Price:  makerOrder.Price,
				Qty:    qty,
				Amount: makerAmount,
			}
		}
		matchedRecord.Taker = *takerOrder
		matchedRecord.Maker = *makerOrder
		matchedRecord.MatchedRecordID = cast.ToString(idgen.NextId())
		matchedResult.MatchedRecords = append(matchedResult.MatchedRecords, matchedRecord)

	}
	//删除买盘被匹配过的订单，更新卖一价
	if len(deletedKeys) > 0 {
		for _, v := range deletedKeys {
			m.bids.orderBook.Remove(v)
		}
		m.updateBestBid()

	}
	//如果taker还是部分匹配，将订单加入的卖盘中
	if takerOrder.OrderStatus == enum.OrderStatus_PartFilled {

		m.addOrder(takerOrder)
		p := &position{
			price: takerOrder.Price,
			qty:   takerOrder.UnfilledBaseAmount,
		}
		m.depthHandler.updateDepth(p, enum.Side_Sell, Add, m.currentSeqId)
	}
	//更新深度数据
	for _, record := range matchedResult.MatchedRecords {
		p := &position{
			price: record.Price,
			qty:   record.Qty,
		}
		log.Printf("%+v", p.castToPosition(5, 6))
		m.depthHandler.updateDepth(p, enum.Side_Buy, Delete, m.currentSeqId)
	}
	matchedResult.MatchTime = time.Now().UnixNano()
	matchedResult.MatchID = cast.ToString(idgen.NextId())
	//m.Next <- matchedResult
	m.SendMatchResult(matchedResult)

}
func (m *MatchEngine) HandleOrder(order *Order) {

	//从接收输入的第一个订单开始，以后每次操作版本号加一
	if m.currentSeqId != 0 {
		m.currentSeqId = order.SequenceId
	} else {
		m.currentSeqId++
	}
	k := &Key{
		price: order.Price,
		id:    order.SequenceId,
	}
	var o interface{}
	var found bool
	if order.OrderType == enum.OrderType_LO {
		if order.Side == enum.Side_Sell {
			o, found = m.asks.orderBook.Get(k)
		} else {
			o, found = m.bids.orderBook.Get(k)
		}
	}
	//判断订单是否存在
	if (order.IsCancel && !found) || (!order.IsCancel && found) {
		return
	}

	if order.IsCancel {
		orderDetail := o.(*Order)
		order.UnfilledBaseAmount = orderDetail.UnfilledBaseAmount
		order.UnfilledQuoteAmount = orderDetail.UnfilledQuoteAmount
		order.QuoteAmount = orderDetail.QuoteAmount
		order.BaseAmount = orderDetail.BaseAmount
		//订单簿删除订单
		m.cancelOrder(order)
		//更新盘口深度
		m.depthHandler.updateDepth(&position{
			price: order.Price,
			qty:   order.UnfilledBaseAmount,
		}, order.Side, Delete, m.currentSeqId)
		//发送取消订单消息

		coinId, qty := m.c.SymbolInfo.BaseCoinID, order.UnfilledBaseAmount.String()
		if orderDetail.Side == enum.Side_Buy {
			coinId = m.c.SymbolInfo.QuoteCoinID
			qty = order.UnfilledQuoteAmount.String()
		}
		m.SendMatchResult(&MatchResult{
			CancelResp: &CancelResp{
				CancelId: order.SequenceId,
				CoinId:   coinId,
				Amount:   qty,
				Uid:      orderDetail.Uid,
			},
			MatchTime: time.Now().UnixNano(),
		})
	} else {
		logx.Debugf("order = %+v bestBid = %v bestAsk=%v", order, m.bestBid, m.bestAsk)
		switch {
		//买单市价单
		case order.Side == enum.Side_Buy && order.OrderType == enum.OrderType_MO:
			m.matchMarkerOrderBuy(order)
		//买单限价单
		case order.Side == enum.Side_Buy && order.OrderType == enum.OrderType_LO:
			//价格大于卖一价，同时卖一价不为零
			if order.Price.GreaterThanOrEqual(m.bestAsk) && m.bestAsk.GreaterThan(utils.DecimalZeroMaxPrec) {
				m.matchLimitOrderBuy(order)
			} else {
				m.addOrder(order)
				//更新盘口深度
				m.depthHandler.updateDepth(&position{
					price: order.Price,
					qty:   order.UnfilledBaseAmount,
				}, order.Side, Add, m.currentSeqId)
			}
		//卖单市价单
		case order.Side == enum.Side_Sell && order.OrderType == enum.OrderType_MO:
			m.matchMarketOrderSell(order)
		//卖单限价单
		case order.Side == enum.Side_Sell && order.OrderType == enum.OrderType_LO:
			if order.Price.LessThanOrEqual(m.bestBid) && m.bestBid.GreaterThan(utils.DecimalZeroMaxPrec) {
				m.matchLimitOrderSell(order)
			} else {
				m.addOrder(order)
				//更新盘口深度
				m.depthHandler.updateDepth(&position{
					price: order.Price,
					qty:   order.UnfilledBaseAmount,
				}, order.Side, Add, m.currentSeqId)
			}
		}
		logx.Debugf(" bestBid = %v bestAsk=%v", m.bestBid, m.bestAsk)
	}

}
func (m *MatchEngine) GetDepth(level int32) DepthData {
	return m.depthHandler.getDepth(level)
}

// SendMatchResult 发送撮合结果，这个操作不异步。
func (m *MatchEngine) SendMatchResult(matchResult *MatchResult) {

	var resp matchMq.MatchResp
	resp.MessageId = stringx.Rand()
	if matchResult.CancelResp != nil {
		resp.Resp = &matchMq.MatchResp_Cancel{
			Cancel: &matchMq.CancelResp{
				Id:     matchResult.CancelResp.CancelId,
				CoinId: matchResult.CancelResp.CoinId,
				Amount: matchResult.CancelResp.Amount,
				Uid:    matchResult.CancelResp.Uid,
			},
		}
	} else {
		beginPrice, endPrice := matchResult.MatchedRecords[0].Price.String(), matchResult.MatchedRecords[len(matchResult.MatchedRecords)-1].Price.String()
		lowPrice, highPrice := beginPrice, endPrice
		if !matchResult.TakerIsBuy {
			highPrice = beginPrice
			lowPrice = endPrice
		}
		records := make([]*matchMq.MatchResult_MatchedRecord, 0, len(matchResult.MatchedRecords))
		totalQty, totalAmount, takerUnFrozenAmount := utils.DecimalZeroMaxPrec, utils.DecimalZeroMaxPrec, utils.DecimalZeroMaxPrec
		for _, record := range matchResult.MatchedRecords {
			//本次撮合一共撮合了多少
			totalQty = totalQty.Add(record.Qty)
			totalAmount = totalAmount.Add(record.Amount)
			takerFilledQty := record.Taker.FilledBaseAmount.String()

			if record.Taker.OrderType == enum.OrderType_LO {
				//taker解冻的金额，以taker的成交价格为准
				a := record.Qty.Mul(record.Taker.Price)
				takerUnFrozenAmount = takerUnFrozenAmount.Add(a)
				takerFilledQty = record.Taker.BaseAmount.Sub(record.Taker.UnfilledBaseAmount).String()

			} else {
				takerUnFrozenAmount = record.Taker.FilledQuoteAmount
			}
			makerFilledBaseAmount := record.Maker.BaseAmount.Sub(record.Maker.UnfilledBaseAmount).String()
			r := &matchMq.MatchResult_MatchedRecord{
				BaseAmount:  record.Qty.String(),
				Price:       record.Price.String(),
				QuoteAmount: record.Amount.String(),
				MatchSubId:  record.MatchedRecordID,
				Taker: &matchMq.OrderResp{
					OrderId:             record.Taker.OrderID,
					FilledBaseAmount:    takerFilledQty,
					UnFilledBaseAmount:  record.Taker.UnfilledBaseAmount.String(),
					FilledQuoteAmount:   record.Taker.FilledQuoteAmount.String(),
					UnFilledQuoteAmount: record.Taker.UnfilledQuoteAmount.String(),
					OrderStatus:         record.Taker.OrderStatus,
					Uid:                 record.Taker.Uid,
					Id:                  record.Taker.SequenceId,
					UnFrozenAmount:      takerUnFrozenAmount.String(),
				},
				Maker: &matchMq.OrderResp{
					OrderId:             record.Maker.OrderID,
					FilledBaseAmount:    makerFilledBaseAmount,
					FilledQuoteAmount:   record.Maker.FilledQuoteAmount.String(),
					UnFilledBaseAmount:  record.Maker.UnfilledBaseAmount.String(),
					OrderStatus:         record.Maker.OrderStatus,
					UnFilledQuoteAmount: record.Maker.UnfilledQuoteAmount.String(),
					Uid:                 record.Maker.Uid,
					Id:                  record.Maker.SequenceId,
				},
			}
			records = append(records, r)
		}
		result := &matchMq.MatchResult{
			SymbolId:      m.c.SymbolInfo.SymbolID,
			SymbolName:    m.c.SymbolInfo.SymbolName,
			BaseCoinId:    m.c.SymbolInfo.BaseCoinID,
			QuoteCoinId:   m.c.SymbolInfo.QuoteCoinID,
			MatchId:       matchResult.MatchID,
			MatchedRecord: records,
			BeginPrice:    beginPrice,
			EndPrice:      endPrice,
			MatchTime:     matchResult.MatchTime,
			Qty:           totalQty.String(),
			Amount:        totalAmount.String(),
			HighPrice:     highPrice,
			LowPrice:      lowPrice,
			TakerIsBuy:    matchResult.TakerIsBuy,
		}
		resp.Resp = &matchMq.MatchResp_MatchResult{
			MatchResult: result,
		}
	}

	logx.Debugw("send match result", logx.Field("data", &resp))
	data, _ := proto.Marshal(&resp)
	var err error
	for i := 0; i < 10; i++ {
		if _, err = m.producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: data,
		}); err != nil {
			logx.Errorw("send message failed", logger.ErrorField(err), logx.Field("count", i+1))
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		logx.Severef("send message failed err=%v", err)
	}

	//m.tick <- matchResult
}

//// 这是一个偷懒的写法，我觉得放到matchmq会比较好。
//func (m *MatchEngine) sendTick() {
//	for matchResult := range m.tick {
//		for _, v := range matchResult.MatchedRecords {
//			tick := commonWs.Tick{
//				Price:        v.Price.StringFixedBank(m.c.SymbolInfo.QuoteCoinPrec.Load()),
//				BaseAmount:          v.BaseAmount.StringFixedBank(m.c.SymbolInfo.BaseCoinPrec.Load()),
//				Amount:       v.Amount.StringFixedBank(m.c.SymbolInfo.QuoteCoinPrec.Load()),
//				TimeStamp:    matchResult.MatchTime / 1e9,
//				TakerIsBuyer: matchResult.TakerIsBuy,
//			}
//			msg := commonWs.Message[commonWs.Tick]{
//				Topic:   commonWs.TickPrefix.WithParam(m.c.SymbolInfo.SymbolName),
//				Payload: tick,
//			}
//			if _, err := m.proxyClient.PushData(context.Background(), &ws.Data{
//				Uid:   "",
//				Topic: commonWs.TickPrefix.WithParam(m.c.SymbolInfo.SymbolName),
//				Data:  msg.ToBytes(),
//			}); err != nil {
//				logx.Errorw("push kline websocket data failed", logger.ErrorField(err), logx.Field("data", tick))
//			}
//		}
//	}
//}
