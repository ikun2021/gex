package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/internal/config"
	"github.com/ikun2021/gex/common/defines"
	"github.com/ikun2021/gex/common/models"
	enum "github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"
	ws "github.com/luxun9527/gpush/proto"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"google.golang.org/protobuf/proto"
)

// MatchEngine 撮合引擎
type MatchEngine struct {
	asks                   *OrderBook      //卖盘
	bids                   *OrderBook      //买盘
	bestBid                decimal.Decimal //买一价
	bestAsk                decimal.Decimal //卖一价
	symbolConf             models.Symbol
	producer               pulsar.Producer
	currentMsgId           int64
	quoteCoinPrecision     int32
	baseCoinPrecision      int32
	redisClient            *redis.Redis
	input                  chan *InputMessage
	storeChan              chan *SnapshotData
	version                int64
	currentPulsarMessageId pulsar.MessageID
	DepthHandler           *DepthHandler
	wsClient               ws.ProxyClient
	Consumer               pulsar.Consumer
}

func (m *MatchEngine) Gte(msgId int64) bool {
	return m.currentMsgId > msgId || m.currentMsgId == msgId
}
func (m *MatchEngine) dump() {
	fmt.Printf("⚙️  ----------------------------------------------撮合引擎详情 (Match Engine)-----------------------------------:\n")
	fmt.Printf("🪙 交易对: %s (ID: %d)\n", m.symbolConf.Name, m.symbolConf.Id)
	fmt.Printf("🪙 基础币ID: %d\n", m.symbolConf.BaseCoinId)
	fmt.Printf("🪙 计价币ID: %d\n", m.symbolConf.QuoteCoinId)
	fmt.Printf("📏 基础币精度: %d\n", m.baseCoinPrecision)
	fmt.Printf("📏 计价币精度: %d\n", m.quoteCoinPrecision)
	fmt.Printf("📈 最佳买价 (买一价): %s\n", m.bestBid.String())
	fmt.Printf("📉 最佳卖价 (卖一价): %s\n", m.bestAsk.String())
	fmt.Println("🛒 卖盘 (ASKS):")
	if m.asks != nil {
		m.asks.dump()
	} else {
		fmt.Println("  无卖盘数据")
	}
	fmt.Println("📥 买盘 (BIDS):")
	if m.bids != nil {
		m.bids.dump()
	} else {
		fmt.Println("  无买盘数据")
	}
}

// MatchedRecord  一次撮合匹配的结果,一次撮合会多次匹配
type MatchedRecord struct {
	Price           decimal.Decimal
	Qty             decimal.Decimal
	Amount          decimal.Decimal
	MatchedRecordID string
	//最新的taker订单的状态
	Taker InputMessage
	//最新的maker订单的状态
	Maker InputMessage
}
type MatchResult struct {
	//每一次匹配的结构
	MatchedRecords []*MatchedRecord
	//本次撮合的id
	MatchID string
	//撮合时间
	MatchTime int64
	//taker为买单
	TakerIsBuy bool
}

func (m *MatchResult) dump() {
	fmt.Printf("-------------------------------📊 撮合结果详情 (Match Result): -------------------------\n")
	fmt.Printf("🆔 撮合ID: %s\n", m.MatchID)
	fmt.Printf("⏰ 撮合时间: %s\n", time.Unix(0, m.MatchTime).Format("2006-01-02 15:04:05.000"))
	fmt.Printf("📈 Taker方向: %s\n", func() string {
		if m.TakerIsBuy {
			return "买入 (BUY)"
		}
		return "卖出 (SELL)"
	}())
	fmt.Printf("🔢 匹配记录数量: %d\n", len(m.MatchedRecords))

	if len(m.MatchedRecords) > 0 {
		fmt.Println("📋 匹配记录详情:")
		for i, record := range m.MatchedRecords {
			fmt.Printf("  记录 #%d:\n", i+1)
			fmt.Printf("    📍 价格: %s\n", record.Price.String())
			fmt.Printf("    📦 数量: %s\n", record.Qty.String())
			fmt.Printf("    💰 金额: %s\n", record.Amount.String())
			fmt.Printf("    🔑 匹配ID: %s\n", record.MatchedRecordID)
		}
	}
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

func (a *AcceptedResult) dump() {
	fmt.Printf("✅ 结果订单接受详情 (Accepted Result): \n")
	fmt.Printf("🆔 订单ID: %s\n", a.OrderId)
	fmt.Printf("👤 用户ID: %d\n", a.Uid)
	fmt.Printf("📈 方向: %s\n", func() string {
		switch a.side {
		case enum.Side_Buy:
			return "买入 (BUY)"
		case enum.Side_Sell:
			return "卖出 (SELL)"
		default:
			return "未知 (UNKNOWN)"
		}
	}())
	fmt.Printf("📍 价格: %s\n", a.price)
	fmt.Printf("💰 计价币金额: %s\n", a.quoteAmount)
	fmt.Printf("📦 基础币数量: %s\n", a.baseAmount)
}

type MatchOutputMessage struct {
	MatchResult    *MatchResult
	CancelResult   *CancelResult
	AcceptedResult *AcceptedResult
	MsgType        MsgType
	MsgId          int64
}

func (m *MatchOutputMessage) Dump() {
	fmt.Printf("📤 --------------------------------✅ 消息输出详情 (Match Output Message): --------------------------\n")
	fmt.Printf("🏷️  消息类型: %s (%s)\n", m.MsgType, m.MsgType.String())

	switch m.MsgType {
	case MsgTypeMatchResult:
		if m.MatchResult != nil {
			m.MatchResult.dump()
		} else {
			fmt.Println("⚠️  撮合结果为空")
		}
	case MsgTypeCancelResult:
		if m.CancelResult != nil {
			m.CancelResult.dump()
		} else {
			fmt.Println("⚠️  取消结果为空")
		}
	case MsgTypeAcceptedResult:
		if m.AcceptedResult != nil {
			m.AcceptedResult.dump()
		} else {
			fmt.Println("⚠️  接受结果为空")
		}
	default:
		fmt.Printf("❓ 未知消息类型: %d\n", m.MsgType)
	}
}

type MsgType int8

const (
	MsgTypeMatchResult MsgType = iota + 1
	MsgTypeCancelResult
	MsgTypeAcceptedResult
)

func (msgType MsgType) String() string {
	switch msgType {
	case MsgTypeMatchResult:
		return "撮合结果"
	case MsgTypeCancelResult:
		return "订单取消结果"
	default:
		return "订单接受"
	}
}

type CancelResult struct {
	//取消订单的id，如果不为空则表示取消订单。
	CancelId int64
	//币种id 取消买单则为计价币id，取消卖单则为基础币id
	CoinId int32
	//数量 取消买单则为计价币数量，取消卖单则为基础币数量
	Amount string
	//用户id
	Uid       int64
	Ts        int64
	Side      enum.Side
	OrderType enum.OrderType
}

func (c *CancelResult) dump() {
	fmt.Printf("🚫 ---------------------------------订单取消详情 (Cancel Result): ---------------------------------\n")
	fmt.Printf("🆔 取消订单ID: %d\n", c.CancelId)
	fmt.Printf("🪙 币种ID: %d\n", c.CoinId)
	fmt.Printf("💰 取消数量: %s\n", c.Amount)
	fmt.Printf("👤 用户ID: %d\n", c.Uid)
	fmt.Printf("⏰ 取消时间: %s\n", time.Unix(0, c.Ts).Format("2006-01-02 15:04:05.000"))
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

func NewMatchEngine(c models.Symbol, conf config.Config, producer pulsar.Producer, consumer pulsar.Consumer, redisClient *redis.Redis, client ws.ProxyClient) *MatchEngine {
	me := &MatchEngine{
		asks:               NewOrderBook(enum.Side_Sell),
		bids:               NewOrderBook(enum.Side_Buy),
		bestBid:            utils.DecimalZeroMaxPrec,
		bestAsk:            utils.DecimalZeroMaxPrec,
		symbolConf:         c,
		producer:           producer,
		quoteCoinPrecision: conf.Coin[c.QuoteCoinId-1].Precision,
		baseCoinPrecision:  conf.Coin[c.BaseCoinId-1].Precision,
		redisClient:        redisClient,
		input:              make(chan *InputMessage, 1000),
		storeChan:          make(chan *SnapshotData, 1000),
		wsClient:           client,
		Consumer:           consumer,
	}
	me.recover()
	return me
}

func (m *MatchEngine) addOrder(order *InputMessage) {
	if order.Side == enum.Side_Buy {
		m.bids.add(order)
		m.updateBestBid()
	} else {
		m.asks.add(order)
		m.updateBestAsk()
	}

}
func (m *MatchEngine) cancelOrder(order *InputMessage) {
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
func (m *MatchEngine) matchMarketOrderSell(takerOrder *InputMessage) {

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
			CancelId: takerOrder.OrderPkId,
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
		makerOrder := iterator.Value().(*InputMessage)
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
			CancelResult: &CancelResult{
				CancelId: takerOrder.OrderPkId,
				CoinId:   m.symbolConf.BaseCoinId,
				Amount:   takerOrder.UnfilledBaseAmount.String(),
				Uid:      takerOrder.Uid,
			},
			MsgType: MsgTypeCancelResult,
		}
		//发送取消订单
		m.SendResult(r)

	}
}

// 市价买单 按照指定计价币的数量来买
func (m *MatchEngine) matchMarkerOrderBuy(takerOrder *InputMessage) {

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
			CancelId: takerOrder.OrderPkId,
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
		makerOrder := iterator.Value().(*InputMessage)
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
	for _, record := range matchMsg.MatchResult.MatchedRecords {
		p := &position{
			price: record.Price,
			qty:   record.Qty,
		}
		m.DepthHandler.updateDepth(p, enum.Side_Sell, Delete, m.version)
	}
	matchMsg.MatchResult.MatchTime = time.Now().UnixNano()
	if len(matchMsg.MatchResult.MatchedRecords) > 0 {
		m.SendResult(matchMsg)
	}

	if takerOrder.OrderStatus != enum.OrderStatus_ALLFilled {
		r := &MatchOutputMessage{CancelResult: &CancelResult{
			CancelId: takerOrder.OrderPkId,
			CoinId:   m.symbolConf.QuoteCoinId,
			Amount:   takerOrder.UnfilledQuoteAmount.String(),
			Uid:      takerOrder.Uid,
		}}
		//发送取消订单
		m.SendResult(r)

	}
}

// 匹配限价买单
func (m *MatchEngine) matchLimitOrderBuy(takerOrder *InputMessage) {
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
		makerOrder := iterator.Value().(*InputMessage)

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
		m.addOrder(takerOrder)
		p := &position{
			price: takerOrder.Price,
			qty:   takerOrder.UnfilledBaseAmount,
		}
		m.DepthHandler.updateDepth(p, enum.Side_Buy, Add, m.currentMsgId)
	} //更新深度数据
	for _, record := range matchMsg.MatchResult.MatchedRecords {
		p := &position{
			price: record.Price,
			qty:   record.Qty,
		}
		m.DepthHandler.updateDepth(p, enum.Side_Sell, Delete, m.currentMsgId)
	}
	//更新深度数据

	matchMsg.MatchResult.MatchTime = time.Now().UnixNano()
	matchMsg.MatchResult.MatchID = cast.ToString(idgen.NextId())
	//发送撮合结果
	m.SendResult(matchMsg)

}

// 匹配限价卖单
func (m *MatchEngine) matchLimitOrderSell(takerOrder *InputMessage) {
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
		makerOrder := iterator.Value().(*InputMessage)
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
		p := &position{
			price: takerOrder.Price,
			qty:   takerOrder.UnfilledBaseAmount,
		}
		m.DepthHandler.updateDepth(p, enum.Side_Sell, Add, m.currentMsgId)

	}
	//更新深度数据
	for _, record := range matchMessage.MatchResult.MatchedRecords {
		p := &position{
			price: record.Price,
			qty:   record.Qty,
		}
		m.DepthHandler.updateDepth(p, enum.Side_Buy, Delete, m.currentMsgId)
	}
	matchMessage.MatchResult.MatchTime = time.Now().UnixNano()
	matchMessage.MatchResult.MatchID = cast.ToString(idgen.NextId())
	m.SendResult(matchMessage)

}

func (m *MatchEngine) Start() {
	m.store()
	ticker := time.NewTicker(time.Second * 10)
	isUpdated := false
	go func() {
		for {
			select {
			case <-ticker.C:
				if isUpdated {
					//执行持久化
					m.snapshot()
					isUpdated = false
				}
			case order := <-m.input:
				m.version++
				isUpdated = true
				m.currentPulsarMessageId = order.PulsarMsgId
				m.currentMsgId = order.MessageId
				m.handle(order)
				if m.version%2000 == 0 {
					m.snapshot()
				}

			}
		}
	}()
}
func (m *MatchEngine) HandleOrder(order *InputMessage) {
	m.input <- order
}

func (m *MatchEngine) store() {
	go func() {
		for snapshotData := range m.storeChan {
			data, _ := json.Marshal(snapshotData)
			if err := m.redisClient.Set("match_engine_snapshot:"+cast.ToString(m.symbolConf.Id), string(data)); err != nil {
				logx.Errorf("store current msg id failed %v", err)
			}
			if err := m.Consumer.AckIDCumulative(snapshotData.PulsarMsgID); err != nil {
				logx.Errorf("store ack msg id failed %v", err)
			}
			logx.Infof("match engine store success symbol=%v msgId=%v", m.symbolConf.Name, m.currentMsgId)
		}
	}()
}

func (m *MatchEngine) snapshot() {
	//持久化
	data := &SnapshotData{
		Asks:         make([]*InputMessage, 0, m.asks.orderBook.Size()),
		Bids:         make([]*InputMessage, 0, m.bids.orderBook.Size()),
		CurrentMsgId: m.currentMsgId,
		PulsarMsgID:  m.currentPulsarMessageId,
	}
	for _, v := range m.asks.orderBook.Values() {
		data.Asks = append(data.Asks, v.(*InputMessage))
	}
	for _, v := range m.bids.orderBook.Values() {
		data.Bids = append(data.Bids, v.(*InputMessage))
	}

	m.storeChan <- data
}

type SnapshotData struct {
	Asks         []*InputMessage  `json:"asks"`
	Bids         []*InputMessage  `json:"bids"`
	CurrentMsgId int64            `json:"current_msg_id"`
	PulsarMsgID  pulsar.MessageID `json:"pulsar_msg_id"`
}

func (m *MatchEngine) recover() {
	key := fmt.Sprintf("match_engine_snapshot:%d", m.symbolConf.Id)
	val, err := m.redisClient.Get(key)
	if err != nil && !errors.Is(err, redis.Nil) {
		logx.Severef("match engine recover failed symbol=%v err=%v", m.symbolConf.Name, err)
	}
	var data SnapshotData
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		logx.Errorf("match engine recover unmarshal failed symbol=%v err=%v", m.symbolConf.Name, err)
	}
	m.DepthHandler = NewDepthHandler(data.CurrentMsgId, &m.symbolConf, m.wsClient)
	for _, v := range data.Asks {
		m.DepthHandler.updateDepth(&position{
			price: v.Price,
			qty:   v.UnfilledBaseAmount,
		}, enum.Side_Sell, Add, v.MessageId)
		m.asks.add(v)
	}
	for _, v := range data.Bids {
		m.DepthHandler.updateDepth(&position{
			price: v.Price,
			qty:   v.UnfilledBaseAmount,
		}, enum.Side_Buy, Delete, v.MessageId)
		m.bids.add(v)
	}
	m.currentMsgId = data.CurrentMsgId
	m.updateBestAsk()
	m.updateBestBid()

	logx.Infof("match engine recover success symbol=%v msgId=%v asks=%d bids=%d", m.symbolConf.Name, m.currentMsgId, len(data.Asks), len(data.Bids))
}

func (m *MatchEngine) handle(inputMsg *InputMessage) {

	k := &Key{
		price: inputMsg.Price,
		id:    inputMsg.OrderPkId,
	}
	var o interface{}
	var found bool
	if inputMsg.OrderType == enum.OrderType_LO {
		if inputMsg.Side == enum.Side_Sell {
			o, found = m.asks.orderBook.Get(k)
		} else {
			o, found = m.bids.orderBook.Get(k)
		}
	}
	//判断订单是否存在
	//取消的话存在并且
	//新增的时候
	if (inputMsg.IsCancel && !found) || (!inputMsg.IsCancel && found) {
		return
	}

	if inputMsg.IsCancel {
		orderDetail := o.(*InputMessage)
		inputMsg.UnfilledBaseAmount = orderDetail.UnfilledBaseAmount
		inputMsg.UnfilledQuoteAmount = orderDetail.UnfilledQuoteAmount
		inputMsg.QuoteAmount = orderDetail.QuoteAmount
		inputMsg.BaseAmount = orderDetail.BaseAmount
		//订单簿删除订单
		m.cancelOrder(inputMsg)
		//更新盘口深度
		m.DepthHandler.updateDepth(&position{
			price: inputMsg.Price,
			qty:   inputMsg.UnfilledBaseAmount,
		}, inputMsg.Side, Delete, 0)
		//发送取消订单消息

		coinId, qty := m.symbolConf.BaseCoinId, inputMsg.UnfilledBaseAmount.String()
		if orderDetail.Side == enum.Side_Buy {
			coinId = m.symbolConf.QuoteCoinId
			qty = inputMsg.UnfilledQuoteAmount.String()
		}
		m.SendResult(&MatchOutputMessage{
			CancelResult: &CancelResult{
				CancelId:  inputMsg.OrderPkId,
				CoinId:    coinId,
				Amount:    qty,
				Uid:       orderDetail.Uid,
				Ts:        0,
				Side:      inputMsg.Side,
				OrderType: inputMsg.OrderType,
			},
			MsgType: MsgTypeCancelResult,
			MsgId:   m.currentMsgId,
		})
	} else {
		logx.Debugf("处理之前")
		m.dump()
		switch {
		//买单市价单
		case inputMsg.Side == enum.Side_Buy && inputMsg.OrderType == enum.OrderType_MO:
			m.matchMarkerOrderBuy(inputMsg)
		//买单限价单,发送一条accepted消息
		case inputMsg.Side == enum.Side_Buy && inputMsg.OrderType == enum.OrderType_LO:

			//价格大于卖一价，同时卖一价不为零
			if inputMsg.Price.GreaterThanOrEqual(m.bestAsk) && m.bestAsk.GreaterThan(utils.DecimalZeroMaxPrec) {
				m.matchLimitOrderBuy(inputMsg)
			} else {

				m.addOrder(inputMsg)
				//更新盘口深度
				m.DepthHandler.updateDepth(&position{
					price: inputMsg.Price,
					qty:   inputMsg.UnfilledBaseAmount,
				}, inputMsg.Side, Add, 0)
				//更新盘口深度
			}
		//卖单市价单
		case inputMsg.Side == enum.Side_Sell && inputMsg.OrderType == enum.OrderType_MO:
			m.matchMarketOrderSell(inputMsg)
		//买单限价单,发送一条accepted消息
		case inputMsg.Side == enum.Side_Sell && inputMsg.OrderType == enum.OrderType_LO:
			m.SendResult(&MatchOutputMessage{
				AcceptedResult: &AcceptedResult{
					OrderId:     inputMsg.OrderID,
					Uid:         inputMsg.Uid,
					side:        inputMsg.Side,
					price:       inputMsg.Price.String(),
					quoteAmount: inputMsg.QuoteAmount.String(),
					baseAmount:  inputMsg.BaseAmount.String(),
				},
				MsgType: MsgTypeAcceptedResult,
			})

			if inputMsg.Price.LessThanOrEqual(m.bestBid) && m.bestBid.GreaterThan(utils.DecimalZeroMaxPrec) {
				m.matchLimitOrderSell(inputMsg)
			} else {

				m.addOrder(inputMsg)
				//更新盘口深度
				//更新盘口深度
				m.DepthHandler.updateDepth(&position{
					price: inputMsg.Price,
					qty:   inputMsg.UnfilledBaseAmount,
				}, inputMsg.Side, Add, 0)

			}
		}
		logx.Debugf("处理之后")
		m.dump()
	}

}

// SendResult 发送撮合结果，这个操作不异步。
func (m *MatchEngine) SendResult(matchMsg *MatchOutputMessage) {
	var resp matchMq.MatchOutput
	resp.MessageId = m.currentMsgId
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
		totalQty, totalAmount := utils.DecimalZeroMaxPrec, utils.DecimalZeroMaxPrec
		for _, record := range matchResult.MatchedRecords {
			//本次撮合一共撮合了多少
			totalQty = totalQty.Add(record.Qty)
			totalAmount = totalAmount.Add(record.Amount)
			takerFilledQty := record.Taker.FilledBaseAmount.String()

			// 本笔成交对应的解冻量（按冻结币种：买单解冻计价币，卖单解冻基础币）
			var takerUnFrozenAmount decimal.Decimal
			if record.Taker.OrderType == enum.OrderType_LO {
				if matchResult.TakerIsBuy {
					takerUnFrozenAmount = record.Qty.Mul(record.Taker.Price)
				} else {
					takerUnFrozenAmount = record.Qty
				}
				takerFilledQty = record.Taker.BaseAmount.Sub(record.Taker.UnfilledBaseAmount).String()
			} else if matchResult.TakerIsBuy {
				takerUnFrozenAmount = record.Amount
			} else {
				takerUnFrozenAmount = record.Qty
			}

			var makerUnFrozenAmount decimal.Decimal
			if record.Maker.OrderType == enum.OrderType_LO {
				if matchResult.TakerIsBuy {
					makerUnFrozenAmount = record.Qty
				} else {
					makerUnFrozenAmount = record.Qty.Mul(record.Maker.Price)
				}
			} else if matchResult.TakerIsBuy {
				makerUnFrozenAmount = record.Qty
			} else {
				makerUnFrozenAmount = record.Amount
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
					Id:                  record.Taker.OrderPkId,
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
					Id:                  record.Maker.OrderPkId,
					UnFrozenAmount:      makerUnFrozenAmount.String(),
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
			BaseAmount:    totalQty.String(),
			QuoteAmount:   totalAmount.String(),
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
				OrderId:   cast.ToString(int32(matchMsg.CancelResult.OrderType)) + cast.ToString(int32(matchMsg.CancelResult.Side)) + cast.ToString(matchMsg.CancelResult.CancelId),
				Id:        matchMsg.CancelResult.CancelId,
				CoinId:    matchMsg.CancelResult.CoinId,
				Amount:    matchMsg.CancelResult.Amount,
				Uid:       matchMsg.CancelResult.Uid,
				Side:      matchMsg.CancelResult.Side,
				OrderType: matchMsg.CancelResult.OrderType,
			},
		}
		logx.Infof("send cancel result %+v to topic %s", &resp, m.producer.Topic())
	case MsgTypeAcceptedResult:

	}

	logx.Infof("send match result %v to topic %s", &resp, m.producer.Topic())
	data, _ := proto.Marshal(&resp)

	var err error
	for i := 1; true; i++ {
		if _, err = m.producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: data,
		}); err != nil {
			logx.Errorw("send message failed", logger.ErrorField(err), logx.Field("count", i+1))
			time.Sleep(time.Second * 3)
			continue
		}
		break
	}

}
