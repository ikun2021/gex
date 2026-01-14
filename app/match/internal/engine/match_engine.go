package engine

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/internal/config"
	"github.com/ikun2021/gex/common/defines"
	"github.com/ikun2021/gex/common/models"
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
	"time"
)

// MatchEngine 撮合引擎
type MatchEngine struct {
	asks               *OrderBook      //卖盘
	bids               *OrderBook      //买盘
	bestBid            decimal.Decimal //买一价
	bestAsk            decimal.Decimal //卖一价
	symbolConf         models.Symbol
	producer           pulsar.Producer
	proxyClient        ws.ProxyClient
	currentSeqId       int64
	quoteCoinPrecision int32
	baseCoinPrecision  int32
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
	CancelResp *CancelResult
	//撮合时间
	MatchTime int64
	//taker为买单
	TakerIsBuy bool
}
type AcceptedResult struct {
	//订单id
	OrderId string
	//用户id
	Uid int64
	//方向
	side enum.Side
	//价格
	price string
	//计价币
	quoteAmount string
	//基础币数量
	baseAmount string
}

type MatchOutputMessage struct {
	MatchResult    *MatchResult
	CancelResult   *CancelResult
	AcceptedResult *AcceptedResult
	MsgType        MsgType
}
type MsgType int8

const (
	MsgTypeMatchResult MsgType = iota + 1
	MsgTypeCancelResult
	MsgTypeAcceptedResult
)

type CancelResult struct {
	//取消订单的id，如果不为空则表示取消订单。
	CancelId int64
	//币种id 取消买单则为计价币id，取消卖单则为基础币id
	CoinId int32
	//数量 取消买单则为计价币数量，取消卖单则为基础币数量
	Amount string
	//用户id
	Uid int64
	Ts  int64
}

type AcceptedResp struct {
	//取消订单的id，如果不为空则表示取消订单。
	CancelId int64
	//币种id 取消买单则为计价币id，取消卖单则为基础币id
	CoinId int32
	//数量 取消买单则为计价币数量，取消卖单则为基础币数量
	Amount string
	//用户id
	Uid int64
}

func NewMatchEngine(c models.Symbol, conf config.Config, producer pulsar.Producer) *MatchEngine {
	me := &MatchEngine{
		asks:               NewOrderBook(enum.Side_Sell),
		bids:               NewOrderBook(enum.Side_Buy),
		bestBid:            utils.DecimalZeroMaxPrec,
		bestAsk:            utils.DecimalZeroMaxPrec,
		symbolConf:         c,
		producer:           producer,
		quoteCoinPrecision: conf.Coin[c.QuoteCoinId-1].Precision,
		baseCoinPrecision:  conf.Coin[c.BaseCoinId-1].Precision,
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

	matchMsg := &MatchOutputMessage{
		MatchResult: &MatchResult{
			MatchedRecords: make([]*MatchedRecord, 0, 2),
			TakerIsBuy:     false,
		},
		MsgType: MsgTypeMatchResult,
	}
	//如果没有买盘，直接取消订单
	if m.bids.orderBook.Size() == 0 {
		matchMsg.CancelResult = &CancelResult{
			CancelId: takerOrder.SequenceId,
			CoinId:   m.symbolConf.BaseCoinId,
			Amount:   takerOrder.UnfilledBaseAmount.String(),
			Uid:      takerOrder.Uid,
		}
		matchMsg.MsgType = MsgTypeCancelResult
		m.SendResult(matchMsg)
		return
	}

	iterator := m.bids.orderBook.Iterator()
	var matchedRecord *MatchedRecord
	deletedKeys := make([]*Key, 0, 2)
	for iterator.Next() {
		makerOrder := iterator.Value().(*Order)
		result := takerOrder.UnfilledBaseAmount.Cmp(makerOrder.UnfilledBaseAmount)
		switch result {
		case defines.Gt:
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
		case defines.Eq:
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
		case defines.Lt:
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
		matchMsg.MatchResult.MatchedRecords = append(matchMsg.MatchResult.MatchedRecords, matchedRecord)
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
	matchMsg.MatchResult.MatchTime = time.Now().UnixNano()
	matchMsg.MatchResult.MatchID = cast.ToString(idgen.NextId())
	m.SendResult(matchMsg)

	if takerOrder.OrderStatus != enum.OrderStatus_ALLFilled {
		r := &MatchOutputMessage{
			MatchResult: &MatchResult{
				CancelResp: &CancelResult{
					CancelId: takerOrder.SequenceId,
					CoinId:   m.symbolConf.BaseCoinId,
					Amount:   takerOrder.UnfilledBaseAmount.String(),
					Uid:      takerOrder.Uid,
				},
			},
			MsgType: MsgTypeCancelResult,
		}
		//发送取消订单
		m.SendResult(r)

	}
}

// 市价买单 按照指定计价币的数量来买
func (m *MatchEngine) matchMarkerOrderBuy(takerOrder *Order) {

	matchMsg := &MatchOutputMessage{
		MatchResult: &MatchResult{
			MatchedRecords: make([]*MatchedRecord, 0, 2),
			TakerIsBuy:     true,
		},
		MsgType: MsgTypeMatchResult,
	}
	//如果没有卖盘，直接取消订单
	if m.asks.orderBook.Size() == 0 {
		matchMsg.CancelResult = &CancelResult{
			CancelId: takerOrder.SequenceId,
			CoinId:   m.symbolConf.QuoteCoinId,
			Amount:   takerOrder.UnfilledQuoteAmount.String(),
			Uid:      takerOrder.Uid,
		}
		matchMsg.MsgType = MsgTypeCancelResult
		m.SendResult(matchMsg)
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
		case defines.Gt:
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
		case defines.Eq:
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

		case defines.Lt:
			//taker金额比maker的金额要小，匹配结束
			//按照taker的金额购买的话能买多少，小于最小的单位则结束。
			qty := takerOrder.UnfilledQuoteAmount.Div(makerOrder.Price)
			baseCoinMinUnit := decimal.New(1, -m.baseCoinPrecision)
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
		matchMsg.MatchResult.MatchedRecords = append(matchMsg.MatchResult.MatchedRecords, matchedRecord)
	}
	matchMsg.MatchResult.MatchID = cast.ToString(idgen.NextId())
	//删除买盘中的被匹配完的订单，同时更新卖一价
	if len(deletedKeys) > 0 {
		for _, v := range deletedKeys {
			m.asks.orderBook.Remove(v)
		}
		m.updateBestAsk()
	}
	//更新深度数据

	matchMsg.MatchResult.MatchTime = time.Now().UnixNano()
	//m.Next <- matchMsg
	if len(matchMsg.MatchResult.MatchedRecords) > 0 {
		m.SendResult(matchMsg)
	}

	if takerOrder.OrderStatus != enum.OrderStatus_ALLFilled {
		r := &MatchOutputMessage{MatchResult: &MatchResult{
			CancelResp: &CancelResult{
				CancelId: takerOrder.SequenceId,
				CoinId:   m.symbolConf.QuoteCoinId,
				Amount:   takerOrder.UnfilledQuoteAmount.String(),
				Uid:      takerOrder.Uid,
			},
		}}
		//发送取消订单
		m.SendResult(r)

	}
}

// 匹配限价买单
func (m *MatchEngine) matchLimitOrderBuy(takerOrder *Order) {
	matchMsg := &MatchOutputMessage{
		MatchResult: &MatchResult{
			MatchedRecords: make([]*MatchedRecord, 0, 2),
			TakerIsBuy:     true,
		},
		MsgType: MsgTypeMatchResult,
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
		switch result {
		//吃完maker
		case defines.Gt:
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

		case defines.Eq:
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
		case defines.Lt:
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
		matchMsg.MatchResult.MatchedRecords = append(matchMsg.MatchResult.MatchedRecords, matchedRecord)

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

	}
	//更新深度数据

	matchMsg.MatchResult.MatchTime = time.Now().UnixNano()
	matchMsg.MatchResult.MatchID = cast.ToString(idgen.NextId())
	//发送撮合结果
	m.SendResult(matchMsg)

}

// 匹配限价卖单
func (m *MatchEngine) matchLimitOrderSell(takerOrder *Order) {
	matchMessage := &MatchOutputMessage{
		MatchResult: &MatchResult{
			MatchedRecords: make([]*MatchedRecord, 0, 2),
			TakerIsBuy:     false,
		},
		MsgType: MsgTypeMatchResult,
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
		switch result {
		case defines.Gt:
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
		case defines.Eq:
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
		case defines.Lt:
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
		matchMessage.MatchResult.MatchedRecords = append(matchMessage.MatchResult.MatchedRecords, matchedRecord)

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

	}
	//更新深度数据

	matchMessage.MatchResult.MatchTime = time.Now().UnixNano()
	matchMessage.MatchResult.MatchID = cast.ToString(idgen.NextId())
	m.SendResult(matchMessage)

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

		//发送取消订单消息

		coinId, qty := m.symbolConf.BaseCoinId, order.UnfilledBaseAmount.String()
		if orderDetail.Side == enum.Side_Buy {
			coinId = m.symbolConf.QuoteCoinId
			qty = order.UnfilledQuoteAmount.String()
		}
		m.SendResult(&MatchOutputMessage{
			CancelResult: &CancelResult{
				CancelId: order.SequenceId,
				CoinId:   coinId,
				Amount:   qty,
				Uid:      orderDetail.Uid,
			},
			MsgType: MsgTypeCancelResult,
		})
	} else {
		logx.Debugf("order = %+v bestBid = %v bestAsk=%v", order, m.bestBid, m.bestAsk)
		switch {
		//买单市价单
		case order.Side == enum.Side_Buy && order.OrderType == enum.OrderType_MO:
			m.matchMarkerOrderBuy(order)
		//买单限价单,发送一条accepted消息
		case order.Side == enum.Side_Buy && order.OrderType == enum.OrderType_LO:
			m.SendResult(&MatchOutputMessage{
				MatchResult:  nil,
				CancelResult: nil,
				AcceptedResult: &AcceptedResult{
					OrderId:     order.OrderID,
					Uid:         order.Uid,
					side:        order.Side,
					price:       order.Price.String(),
					quoteAmount: order.QuoteAmount.String(),
					baseAmount:  order.BaseAmount.String(),
				},
				MsgType: MsgTypeAcceptedResult,
			})

			//价格大于卖一价，同时卖一价不为零
			if order.Price.GreaterThanOrEqual(m.bestAsk) && m.bestAsk.GreaterThan(utils.DecimalZeroMaxPrec) {
				m.matchLimitOrderBuy(order)
			} else {
				m.addOrder(order)
				//更新盘口深度
			}
		//卖单市价单
		case order.Side == enum.Side_Sell && order.OrderType == enum.OrderType_MO:
			m.matchMarketOrderSell(order)
		//买单限价单,发送一条accepted消息
		case order.Side == enum.Side_Sell && order.OrderType == enum.OrderType_LO:
			m.SendResult(&MatchOutputMessage{
				MatchResult:  nil,
				CancelResult: nil,
				AcceptedResult: &AcceptedResult{
					OrderId:     order.OrderID,
					Uid:         order.Uid,
					side:        order.Side,
					price:       order.Price.String(),
					quoteAmount: order.QuoteAmount.String(),
					baseAmount:  order.BaseAmount.String(),
				},
				MsgType: MsgTypeAcceptedResult,
			})

			if order.Price.LessThanOrEqual(m.bestBid) && m.bestBid.GreaterThan(utils.DecimalZeroMaxPrec) {
				m.matchLimitOrderSell(order)
			} else {
				m.addOrder(order)
				//更新盘口深度

			}
		}
		logx.Debugf(" bestBid = %v bestAsk=%v", m.bestBid, m.bestAsk)
	}

}

// SendResult 发送撮合结果，这个操作不异步。
func (m *MatchEngine) SendResult(matchMsg *MatchOutputMessage) {

	var resp matchMq.MatchOutput
	resp.MessageId = stringx.Rand()
	switch matchMsg.MsgType {
	case MsgTypeMatchResult:
		matchResult := matchMsg.MatchResult
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
			SymbolId:      m.symbolConf.Id,
			SymbolName:    m.symbolConf.Name,
			BaseCoinId:    m.symbolConf.BaseCoinId,
			QuoteCoinId:   m.symbolConf.QuoteCoinId,
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
		resp.Result = &matchMq.MatchOutput_MatchResult{
			MatchResult: result,
		}
	case MsgTypeCancelResult:
		resp.Result = &matchMq.MatchOutput_CancelResult{
			CancelResult: &matchMq.CancelResult{
				Id:     matchMsg.CancelResult.CancelId,
				CoinId: matchMsg.CancelResult.CoinId,
				Amount: matchMsg.CancelResult.Amount,
				Uid:    matchMsg.CancelResult.Uid,
			},
		}
	case MsgTypeAcceptedResult:

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

}
