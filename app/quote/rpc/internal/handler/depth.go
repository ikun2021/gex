package handler

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	gpush "github.com/luxun9527/gpush/proto"
	ws "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

type DepthHandler struct {
	asks                *rbt.Tree
	bids                *rbt.Tree
	t                   *time.Ticker
	asksChangedPosition map[string]*Position
	bidsChangedPosition map[string]*Position
	plock               sync.RWMutex
	ChangedPosition     chan DepthData
	paramChan           chan *param
	proxyClient         ws.ProxyClient
	symbolInfo          models.Symbol
	consumer            pulsar.Consumer
	currentVersion, //当前版本
	lastVersion int64 //上一个版本
}

type DepthData struct {
	Asks           []*Position
	Bids           []*Position
	LastVersion    int64
	CurrentVersion int64
}

func NewDepthHandler(svcCtx *svc.ServiceContext, consumer pulsar.Consumer, symbolInfo models.Symbol) *DepthHandler {
	dh := &DepthHandler{
		asks:                rbt.NewWith(DepthComparator),
		bids:                rbt.NewWith(DepthComparator),
		t:                   time.NewTicker(time.Second),
		asksChangedPosition: make(map[string]*Position, 10),
		bidsChangedPosition: make(map[string]*Position, 10),
		plock:               sync.RWMutex{},
		paramChan:           make(chan *param, 10),
		ChangedPosition:     make(chan DepthData, 10),
		symbolInfo:          symbolInfo,
		consumer:            consumer,
	}
	go dh.handeUpdateDepth()
	go dh.pushChangedPosition()
	return dh
}

type opType int8

const (
	Add opType = iota + 1
	Delete
)

type param struct {
	p       *position
	side    enum.Side
	op      opType
	version int64
}
type Position struct {
	BaseAmount  string
	Price       string
	QuoteAmount string
}
type position struct {
	price  decimal.Decimal
	amount decimal.Decimal
}

func (p *position) castToPosition(baseCoinPrec, quoteCoinPrec int32) *Position {
	return &Position{
		BaseAmount:  p.amount.StringFixedBank(baseCoinPrec),
		Price:       p.price.StringFixedBank(quoteCoinPrec),
		QuoteAmount: p.price.Mul(p.amount).StringFixedBank(quoteCoinPrec),
	}
}

// DepthComparator 存储为从大到小
func DepthComparator(a, b interface{}) int {
	aAsserted := a.(decimal.Decimal)
	bAsserted := b.(decimal.Decimal)
	result := aAsserted.Cmp(bAsserted)
	return -result
}

func (d *DepthHandler) loadInitData() {}

func (d *DepthHandler) handeUpdateDepth() {
	for {
		select {
		case par := <-d.paramChan:
			d.plock.Lock()
			var changedPosition *position
			//更新深度
			if par.side == enum.Side_Sell {
				if par.op == Add {
					value, found := d.asks.Get(par.p.price)
					if found {
						pos := value.(*position)
						pos.amount = pos.amount.Add(par.p.amount)
						changedPosition = pos
					} else {
						d.asks.Put(par.p.price, par.p)
						changedPosition = par.p
					}
				} else {
					value, found := d.asks.Get(par.p.price)
					if found {
						pos := value.(*position)
						pos.amount = pos.amount.Sub(par.p.amount)
						if pos.amount.Equal(utils.DecimalZeroMaxPrec) {
							d.asks.Remove(par.p.price)
						}
						changedPosition = pos
					}
				}

			} else {
				if par.op == Add {
					value, found := d.bids.Get(par.p.price)
					if found {
						pos := value.(*position)
						pos.amount = pos.amount.Add(par.p.amount)
						changedPosition = pos
					} else {
						d.bids.Put(par.p.price, par.p)
						changedPosition = par.p
					}

				} else {
					value, found := d.bids.Get(par.p.price)
					if found {
						pos := value.(*position)
						pos.amount = pos.amount.Sub(par.p.amount)
						if pos.amount.Equal(utils.DecimalZeroMaxPrec) {
							d.bids.Remove(par.p.price)
						}
						changedPosition = pos
					}
				}
			}
			d.plock.Unlock()
			if par.side == enum.Side_Buy && changedPosition != nil {
				d.bidsChangedPosition[par.p.price.String()] = changedPosition.castToPosition(d.symbolInfo.BaseCoinPrec, d.symbolInfo.QuoteCoinPrec)
			}

			if par.side == enum.Side_Sell && changedPosition != nil {
				d.asksChangedPosition[par.p.price.String()] = changedPosition.castToPosition(d.symbolInfo.BaseCoinPrec, d.symbolInfo.QuoteCoinPrec)
			}
			d.currentVersion = par.version
		case <-d.t.C:
			//定时发送改变的档位前端及时更新
			if len(d.asksChangedPosition) == 0 && len(d.bidsChangedPosition) == 0 {
				continue
			}
			askPositionList := make([]*Position, 0, len(d.asksChangedPosition))
			for _, v := range d.asksChangedPosition {
				askPositionList = append(askPositionList, &Position{
					BaseAmount:  v.BaseAmount,
					Price:       v.Price,
					QuoteAmount: v.QuoteAmount,
				})
			}
			bidPositionList := make([]*Position, 0, len(d.bidsChangedPosition))
			for _, v := range d.bidsChangedPosition {
				bidPositionList = append(bidPositionList, &Position{
					BaseAmount:  v.BaseAmount,
					Price:       v.Price,
					QuoteAmount: v.QuoteAmount,
				})
			}

			var depthData DepthData
			depthData.LastVersion = d.lastVersion
			depthData.CurrentVersion = d.currentVersion
			depthData.Asks = askPositionList
			depthData.Bids = bidPositionList
			d.ChangedPosition <- depthData
			d.bidsChangedPosition = make(map[string]*Position, 10)
			d.asksChangedPosition = make(map[string]*Position, 10)
			d.lastVersion = d.currentVersion
		}
	}

}

func (d *DepthHandler) Handle(message pulsar.Message) {
	var m matchMq.MatchOutput
	if err := proto.Unmarshal(message.Payload(), &m); err != nil {
		logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
		if err := d.consumer.Ack(message); err != nil {
			logx.Errorw("consumer message failed", logger.ErrorField(err))
		}
		return
	}
	switch t := m.Result.(type) {
	case *matchMq.MatchOutput_AcceptedResult:

		p := &position{
			price:  utils.NewFromString(t.AcceptedResult.Price),
			amount: utils.NewFromString(t.AcceptedResult.BaseAmount),
		}
		d.handle(p, t.AcceptedResult.Side, Add, 0)
	case *matchMq.MatchOutput_CancelResult:
		p := &position{
			price:  utils.NewFromString(t.CancelResult.Amount),
			amount: utils.NewFromString(t.CancelResult.Amount),
		}
		d.handle(p, t.CancelResult.Side, Delete, 0)
	case *matchMq.MatchOutput_MatchResult:
		
	}
}
func (d *DepthHandler) handle(p *position, side enum.Side, op opType, version int64) {
	par := &param{
		p:       p,
		side:    side,
		op:      op,
		version: version,
	}
	logx.Debugf("updateDepth %+v op=%v side=%v", p.castToPosition(d.symbolInfo.BaseCoinPrec, d.symbolInfo.QuoteCoinPrec), op, side)
	d.paramChan <- par
}

// 获取实时深度
func (d *DepthHandler) getDepth(level int32) DepthData {
	d.plock.RLock()
	defer d.plock.RUnlock()
	var depthData DepthData
	asksIter := d.asks.Iterator()
	a := make([]*Position, 0, d.asks.Size())
	b := make([]*Position, 0, d.bids.Size())
	for i := int32(0); asksIter.Next(); i++ {
		if i >= level {
			break
		}
		p := asksIter.Value().(*position)
		a = append(a, p.castToPosition(d.symbolInfo.BaseCoinPrec, d.symbolInfo.QuoteCoinPrec))

	}
	bidsIter := d.bids.Iterator()
	for i := int32(0); bidsIter.Next(); i++ {
		if i >= level {
			break
		}
		p := bidsIter.Value().(*position)
		b = append(b, p.castToPosition(d.symbolInfo.BaseCoinPrec, d.symbolInfo.QuoteCoinPrec))
	}
	depthData.Bids = b
	depthData.Asks = a
	depthData.CurrentVersion = d.lastVersion
	return depthData
}

// 推送变化的档位
func (d *DepthHandler) pushChangedPosition() {
	for data := range d.ChangedPosition {
		asks := make([][]string, 0, len(data.Asks))
		for _, v := range data.Asks {
			m := make([]string, 3)
			m[0] = v.Price
			m[1] = v.BaseAmount
			m[2] = v.QuoteAmount
			asks = append(asks, m)
		}
		bids := make([][]string, 0, len(data.Bids))
		for _, v := range data.Bids {
			m := make([]string, 3)
			m[0] = v.Price
			m[1] = v.BaseAmount
			m[2] = v.QuoteAmount
			bids = append(bids, m)
		}
		depth := commonWs.Depth{
			LastVersion:    cast.ToString(data.LastVersion),
			CurrentVersion: cast.ToString(data.CurrentVersion),
			Symbol:         d.symbolInfo.Name,
			Asks:           asks,
			Bids:           bids,
		}
		msg := commonWs.Message[commonWs.Depth]{
			Topic:   commonWs.DepthPrefix.WithParam(d.symbolInfo.Name),
			Payload: depth,
		}

		d1 := &gpush.Data{
			Uid:   "",
			Topic: msg.Topic,
			Data:  msg.ToBytes(),
		}
		if _, err := d.proxyClient.PushData(context.Background(), d1); err != nil {
			logx.Errorw("push websocket data failed", logger.ErrorField(err))
		}
	}
}
