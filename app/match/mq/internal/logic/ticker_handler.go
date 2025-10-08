package logic

import (
	"context"
	"encoding/json"
	"github.com/ikun2021/gex/app/match/mq/internal/dao/model"
	"github.com/ikun2021/gex/app/match/mq/internal/svc"
	gpush "github.com/luxun9527/gpush/proto"
	"github.com/zeromicro/go-zero/core/logx"
	"time"

	"github.com/ikun2021/gex/common/proto/define"
	"github.com/ikun2021/gex/common/proto/ws"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
)

type TickerHandler struct {
	//写ticker的通道
	writeChan chan model.Ticker
	//发送ticker的通道
	sendChan chan model.Ticker
	//全局实时ticker
	tickerData model.Ticker
	//24小时数据链表
	list *model.List
	//24小时移动的速度
	speed *time.Ticker
	//定时发送的频率
	rate *time.Ticker
	//是否改变
	changed bool

	svcCtx *svc.ServiceContext
}

func (th *TickerHandler) initTickerData() {
	high, low := utils.DecimalZeroMaxPrec, utils.DecimalZeroMaxPrec
	amount, volume := utils.DecimalZeroMaxPrec, utils.DecimalZeroMaxPrec

	if th.list.Empty() {
		th.tickerData = model.Ticker{
			Volume:     utils.DecimalZeroMaxPrec,
			High:       utils.DecimalZeroMaxPrec,
			Low:        utils.DecimalZeroMaxPrec,
			Last24:     utils.DecimalZeroMaxPrec,
			Price:      utils.DecimalZeroMaxPrec,
			Amount:     utils.DecimalZeroMaxPrec,
			Range:      utils.DecimalZeroMaxPrec,
			PriceDelta: utils.DecimalZeroMaxPrec,
		}
		return
	}
	head, _ := th.list.Get(0)
	low = head.Value.StartPrice
	for h := head; h != nil; h = h.Next {
		if h.Value.StartPrice.GreaterThan(high) {
			high = h.Value.StartPrice
		}
		if h.Value.StartPrice.LessThan(low) {
			low = h.Value.StartPrice
		}
		amount.Add(h.Value.Amount)
		volume.Add(h.Value.Volume)
	}

	l, _ := th.list.Get(th.list.Size() - 1)
	th.tickerData = model.Ticker{
		Volume: volume,
		High:   high,
		Low:    low,
		Last24: head.Value.StartPrice,
		Price:  l.Value.StartPrice,
		Amount: amount,
	}
	th.tickerData.Range = th.tickerData.Price.Sub(th.tickerData.Last24).Div(th.tickerData.Last24)
	th.tickerData.PriceDelta = th.tickerData.Price.Sub(th.tickerData.Last24)
}
func NewTickerHandler(svcCtx *svc.ServiceContext) *TickerHandler {

	th := &TickerHandler{
		writeChan: make(chan model.Ticker, 10),
		sendChan:  make(chan model.Ticker, 10),
		speed:     time.NewTicker(time.Second),
		rate:      time.NewTicker(time.Duration(300) * time.Millisecond),
		changed:   false,
		svcCtx:    svcCtx,
		list:      model.NewSinglyList(),
	}
	th.initTickList()
	th.initTickerData()
	return th
}
func InitHandler(svcCtx *svc.ServiceContext) {
	th := NewTickerHandler(svcCtx)
	go th.store()
	go th.send()
	go th.update()
}

func (th *TickerHandler) initTickList() {
	now := time.Now().UnixNano()
	yesterday := now - 24*60*60*1e9
	mo := th.svcCtx.Query.MatchedOrder
	data, err := mo.WithContext(context.Background()).
		Select(mo.ID, mo.Price, mo.Qty, mo.Amount, mo.MatchTime).
		Where(mo.SymbolID.Eq(th.svcCtx.SymbolInfo.SymbolID)).
		Where(mo.MatchTime.Between(yesterday, now)).
		Find()
	if err != nil {
		logx.Severef("init tick list failed %v", err)
	}
	for _, v := range data {
		md := &model.MatchData{
			MatchTime:  v.MatchTime,
			Volume:     utils.NewFromStringMaxPrec(v.Amount),
			Amount:     utils.NewFromStringMaxPrec(v.Qty),
			StartPrice: utils.NewFromStringMaxPrec(v.Price),
			EndPrice:   utils.NewFromStringMaxPrec(v.Price),
			Low:        utils.NewFromStringMaxPrec(v.Price),
			High:       utils.NewFromStringMaxPrec(v.Price),
		}
		th.list.Add(md)
	}

}
func (th *TickerHandler) updateTickerData(matchData *model.MatchData) {

	//初始化第一次成交
	if th.tickerData.Price.Equal(utils.DecimalZeroMaxPrec) {
		th.tickerData = model.Ticker{
			Volume:     matchData.Volume,
			TimeUnix:   matchData.MatchTime,
			Price:      matchData.High,
			High:       matchData.High,
			Low:        matchData.Low,
			Last24:     matchData.Low,
			Amount:     matchData.Amount,
			Range:      utils.DecimalZeroMaxPrec,
			PriceDelta: utils.DecimalZeroMaxPrec,
		}
		return
	}
	if matchData.High.GreaterThan(th.tickerData.High) {
		th.tickerData.High = matchData.High
	}

	if matchData.Low.LessThan(th.tickerData.Low) || th.tickerData.Price.Equal(utils.DecimalZeroMaxPrec) {
		th.tickerData.Low = matchData.Low
	}

	//成交量
	th.tickerData.Volume = th.tickerData.Volume.Add(matchData.Volume)
	//交易额
	th.tickerData.Amount = th.tickerData.Amount.Add(matchData.Amount)

	//当前价格
	th.tickerData.Price = matchData.EndPrice
	th.tickerData.TimeUnix = matchData.MatchTime
	//价格变化数量
	th.tickerData.PriceDelta = th.tickerData.Price.Sub(th.tickerData.Last24)
	//涨跌幅
	th.tickerData.Range = th.tickerData.Price.Sub(th.tickerData.Last24).Div(th.tickerData.Last24)

}
func (th *TickerHandler) send() {
	for data := range th.sendChan {
		wsTicker := data.CastToTickerWsData(th.svcCtx.SymbolInfo)
		msg1 := ws.Message[ws.Ticker]{
			Topic:   commonWs.TickerPrefix.WithParam(th.svcCtx.SymbolInfo.SymbolName),
			Payload: data.CastToTickerWsData(th.svcCtx.SymbolInfo),
		}
		if _, err := th.svcCtx.WsClient.PushData(context.Background(), &gpush.Data{
			Uid:   "",
			Topic: commonWs.TickerPrefix.WithParam(th.svcCtx.SymbolInfo.SymbolName),
			Data:  msg1.ToBytes(),
		}); err != nil {
			logx.Errorw("push ticker websocket data failed", logger.ErrorField(err), logx.Field("data", wsTicker))
			continue
		}
		miniTicker := ws.MiniTicker{
			LatestPrice: wsTicker.Price,
			Range:       wsTicker.Range,
			Symbol:      wsTicker.Symbol,
		}
		msg2 := ws.Message[ws.MiniTicker]{
			Topic:   commonWs.MiniTickerPrefix.WithParam(th.svcCtx.SymbolInfo.SymbolName),
			Payload: miniTicker,
		}
		if _, err := th.svcCtx.WsClient.PushData(context.Background(), &gpush.Data{
			Uid:   "",
			Topic: commonWs.MiniTickerPrefix.WithParam(th.svcCtx.SymbolInfo.SymbolName),
			Data:  msg2.ToBytes(),
		}); err != nil {
			logx.Errorw("push kline websocket data failed", logger.ErrorField(err), logx.Field("data", wsTicker))
			continue
		}
	}
}

func (th *TickerHandler) store() {

	for tickerData := range th.writeChan {
		d, _ := json.Marshal(tickerData.CastToTickerRedisData(th.svcCtx.SymbolInfo))
		var err error
		for i := 0; i < 3; i++ {
			if err = th.svcCtx.RedisClient.Hset(string(define.Ticker), th.svcCtx.SymbolInfo.SymbolName, string(d)); err != nil {
				time.Sleep(time.Second * 3)
				continue
			}
			break
		}
		if err != nil {
			logx.Errorw("write data to redis failed", logger.ErrorField(err))
		}

	}
}

// 移动24小时窗口
func (th *TickerHandler) moveWindow() {
	yesterday := time.Now().AddDate(0, 0, -1).UnixNano()
	max, min := utils.DecimalZeroMaxPrec, utils.DecimalZeroMaxPrec
	highIsChanged, lowIsChanged, index := false, false, 0
	//使用链表保存24小时数据，因为数据是有序的，在删除的时候，可以通过重置头的方式批量删除
	first, _ := th.list.Get(0)
	for e := first; e != nil; e = e.Next {
		md := e.Value
		if md.MatchTime >= yesterday {
			break
		}
		//24小时之前的价格,按时间排序的第一条。
		//减交易量
		if v := th.tickerData.Amount.Sub(md.Amount); v.LessThanOrEqual(utils.DecimalZeroMaxPrec) {
			th.tickerData.Amount = utils.DecimalZeroMaxPrec
		} else {
			th.tickerData.Amount = v
		}
		//减交易额
		if v := th.tickerData.Volume.Sub(md.Volume); v.LessThanOrEqual(utils.DecimalZeroMaxPrec) {
			th.tickerData.Volume = utils.DecimalZeroMaxPrec
		} else {
			th.tickerData.Volume = v
		}

		//判断高是否改变
		if md.High.GreaterThanOrEqual(th.tickerData.High) {
			highIsChanged = true
		}
		//判断低是否改变
		if md.Low.LessThanOrEqual(th.tickerData.Low) {
			lowIsChanged = true
		}
		index++

	}
	//没有变化
	if index == 0 {
		return
	}
	//全部删除
	if index >= th.list.Size() {
		th.list.Clear()
		th.tickerData = model.Ticker{
			Volume:     utils.DecimalZeroMaxPrec,
			TimeUnix:   time.Now().UnixNano(),
			High:       utils.DecimalZeroMaxPrec,
			Low:        utils.DecimalZeroMaxPrec,
			Last24:     utils.DecimalZeroMaxPrec,
			Price:      utils.DecimalZeroMaxPrec,
			Amount:     utils.DecimalZeroMaxPrec,
			Range:      utils.DecimalZeroMaxPrec,
			PriceDelta: utils.DecimalZeroMaxPrec,
		}
		th.changed = true
		return
	}

	//删除过期数据
	if err := th.list.ResetHead(index); err != nil {
		logx.Errorw("reset fail", logger.ErrorField(err))
	}
	//24小时前数据
	head, _ := th.list.Get(0)
	logx.Debugw("delete invalid data", logx.Field("head", head.Value.MatchTime))
	th.tickerData.Last24 = head.Value.StartPrice
	//最大值
	if highIsChanged {
		for h := head; h != nil; h = h.Next {
			if h.Value.High.GreaterThan(max) {
				max = h.Value.High
			}
		}
		th.tickerData.High = max
	}

	//最小值
	if lowIsChanged {
		min = head.Value.Low
		for h := head; h != nil; h = h.Next {
			if h.Value.Low.LessThan(min) {
				min = h.Value.Low
			}
		}
		th.tickerData.Low = min
	}

	//涨跌幅
	th.tickerData.Range = th.tickerData.Price.Sub(th.tickerData.Last24).Div(th.tickerData.Last24)

	//变化数量
	th.tickerData.PriceDelta = th.tickerData.Price.Sub(th.tickerData.Last24)
	th.changed = true
}
func (th *TickerHandler) update() {

	for {
		select {
		//移动24小时时间窗口
		case <-th.speed.C:
			th.moveWindow()
		//根据成交数据更新最新ticker
		case matchData := <-th.svcCtx.MatchDataChan:
			th.updateTickerData(matchData)
			th.list.Add(matchData)
			th.changed = true
		case <-th.rate.C:
			if !th.changed {
				continue
			}
			th.sendChan <- th.tickerData
			th.writeChan <- th.tickerData
			th.changed = false
		}
	}
}
