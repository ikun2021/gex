package handler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao/quote/model"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/proto/define"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"
	gpush "github.com/luxun9527/gpush/proto"
	"github.com/shopspring/decimal"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

type TickerHandler struct {
	consumer      pulsar.Consumer
	svcCtx        *svc.ServiceContext
	symbolInfo    models.Symbol
	kline1mBuffer []*model.MemoryKline
	latestPrice   decimal.Decimal
	latestMatchId int64
	matchData     chan *model.MatchData
}

func NewTickerHandler(svcCtx *svc.ServiceContext, consumer pulsar.Consumer, symbol models.Symbol) *TickerHandler {
	th := &TickerHandler{
		consumer:      consumer,
		svcCtx:        svcCtx,
		symbolInfo:    symbol,
		kline1mBuffer: make([]*model.MemoryKline, 0, 1440),
		latestPrice:   utils.DecimalZeroMaxPrec,
		matchData:     make(chan *model.MatchData, 100),
	}
	th.init()
	go th.start()
	return th
}

func (th *TickerHandler) init() {
	// 加载过去24小时的1分钟K线
	kline := th.svcCtx.GenDB.Kline
	startTime := time.Now().Add(-24 * time.Hour).Unix()
	klines, err := th.svcCtx.GenDB.Kline.WithContext(context.Background()).
		Where(kline.Symbol.Eq(th.symbolInfo.Name),
			kline.KlineType.Eq(int32(model.Min1)),
			kline.StartTime.Lt(startTime)).
		Order(kline.StartTime.Desc()).
		Find()
	if err != nil {
		logx.Errorf("load 24h klines failed err=%v", err)
	}

	for _, k := range klines {
		mk := &model.MemoryKline{
			KlineType: model.Min1,
			StartTime: k.StartTime,
			EndTime:   k.EndTime,
			Amount:    utils.NewFromString(k.Amount),
			Volume:    utils.NewFromString(k.Volume),
			Open:      utils.NewFromString(k.Open),
			High:      utils.NewFromString(k.High),
			Low:       utils.NewFromString(k.Low),
			Close:     utils.NewFromString(k.Close),
		}
		th.kline1mBuffer = append(th.kline1mBuffer, mk)
		th.latestPrice = mk.Close
	}

	// 从Redis读取当前1分钟K线
	data, err := th.svcCtx.RedisClient.Hget(define.Kline.WithParams(), th.symbolInfo.Name+"_"+model.Min1.String())
	if err == nil && data != "" {
		var d model.RedisModel
		if err := json.Unmarshal([]byte(data), &d); err == nil {
			mk := &model.MemoryKline{
				KlineType: model.Min1,
				StartTime: d.StartTime,
				EndTime:   d.EndTime,
				Amount:    utils.NewFromString(d.Amount),
				Volume:    utils.NewFromString(d.Volume),
				Open:      utils.NewFromString(d.Open),
				High:      utils.NewFromString(d.High),
				Low:       utils.NewFromString(d.Low),
				Close:     utils.NewFromString(d.Close),
			}
			// 如果Redis里的K线比DB里的新，则更新
			if len(th.kline1mBuffer) == 0 || mk.StartTime > th.kline1mBuffer[len(th.kline1mBuffer)-1].StartTime {
				th.kline1mBuffer = append(th.kline1mBuffer, mk)
				th.latestPrice = mk.Close
			} else if mk.StartTime == th.kline1mBuffer[len(th.kline1mBuffer)-1].StartTime {
				th.kline1mBuffer[len(th.kline1mBuffer)-1] = mk
				th.latestPrice = mk.Close
			}
		}
	}
}

func (th *TickerHandler) Handle(msg pulsar.Message) {
	var m matchMq.MatchOutput
	if err := proto.Unmarshal(msg.Payload(), &m); err != nil {
		logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
		return
	}

	if th.latestMatchId >= m.MessageId {
		return
	}

	switch r := m.Result.(type) {
	case *matchMq.MatchOutput_MatchResult:
		md := &model.MatchData{
			MatchID:    m.MessageId,
			MatchTime:  r.MatchResult.MatchTime / 1e9,
			Volume:     utils.NewFromString(r.MatchResult.QuoteAmount),
			Amount:     utils.NewFromString(r.MatchResult.BaseAmount),
			StartPrice: utils.NewFromString(r.MatchResult.BeginPrice),
			EndPrice:   utils.NewFromString(r.MatchResult.EndPrice),
			Low:        utils.NewFromString(r.MatchResult.LowPrice),
			High:       utils.NewFromString(r.MatchResult.HighPrice),
		}
		th.matchData <- md
	}
}

func (th *TickerHandler) start() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case md := <-th.matchData:
			th.update(md)
			th.latestMatchId = md.MatchID
		case <-ticker.C:
			th.calculateAndPush()
		}
	}
}

func (th *TickerHandler) update(md *model.MatchData) {
	startTime := md.MatchTime / 60 * 60
	endTime := startTime + 60

	if len(th.kline1mBuffer) > 0 {
		last := th.kline1mBuffer[len(th.kline1mBuffer)-1]
		if startTime == last.StartTime {
			// 更新当前1分钟K线
			if md.High.GreaterThan(last.High) {
				last.High = md.High
			}
			if md.Low.LessThan(last.Low) {
				last.Low = md.Low
			}
			last.Close = md.EndPrice
			last.Amount = last.Amount.Add(md.Amount)
			last.Volume = last.Volume.Add(md.Volume)
			th.latestPrice = md.EndPrice
			return
		}
	}

	// 开启新的一分钟
	mk := &model.MemoryKline{
		KlineType: model.Min1,
		StartTime: startTime,
		EndTime:   endTime,
		Open:      md.StartPrice,
		High:      md.High,
		Low:       md.Low,
		Close:     md.EndPrice,
		Amount:    md.Amount,
		Volume:    md.Volume,
	}
	th.kline1mBuffer = append(th.kline1mBuffer, mk)
	th.latestPrice = md.EndPrice

	// 保持1440个（24小时）
	if len(th.kline1mBuffer) > 1440 {
		th.kline1mBuffer = th.kline1mBuffer[len(th.kline1mBuffer)-1440:]
	}
}

func (th *TickerHandler) calculateAndPush() {
	if len(th.kline1mBuffer) == 0 {
		return
	}

	var (
		high   = decimal.Zero
		low    = decimal.NewFromInt(1e18) // 初始设为一个极大值
		amount = decimal.Zero
		volume = decimal.Zero
	)

	// 仅统计过去24小时内的数据
	now := time.Now().Unix()
	cutoff := now - 24*3600

	firstValidIdx := -1
	for i, k := range th.kline1mBuffer {
		if k.EndTime <= cutoff {
			continue
		}
		if firstValidIdx == -1 {
			firstValidIdx = i
		}
		if k.High.GreaterThan(high) {
			high = k.High
		}
		if k.Low.LessThan(low) {
			low = k.Low
		}
		amount = amount.Add(k.Amount)
		volume = volume.Add(k.Volume)
	}

	if firstValidIdx == -1 {
		return
	}

	openPrice := th.kline1mBuffer[firstValidIdx].Open
	priceRange := "0"
	if !openPrice.IsZero() {
		priceRange = th.latestPrice.Sub(openPrice).Div(openPrice).Mul(decimal.NewFromInt(100)).StringFixed(2)
	}

	ticker := commonWs.Ticker{
		Symbol:          th.symbolInfo.Name,
		Price:           th.latestPrice.String(),
		High:            high.String(),
		Low:             low.String(),
		Amount:          amount.String(),
		Volume:          volume.String(),
		Range:           priceRange,
		Last24HourPrice: openPrice.String(),
	}

	// 推送 WebSocket
	msg := commonWs.Message[commonWs.Ticker]{
		Topic:   commonWs.TickerPrefix.WithParam(th.symbolInfo.Name),
		Payload: ticker,
	}
	data, _ := json.Marshal(msg)
	th.svcCtx.WsClient.PushData(context.Background(), &gpush.Data{
		Topic: msg.Topic,
		Data:  data,
	})

	// 更新 Redis
	tickerData, _ := json.Marshal(ticker)
	th.svcCtx.RedisClient.Hset(define.Ticker.WithParams(), th.symbolInfo.Name, string(tickerData))
}
