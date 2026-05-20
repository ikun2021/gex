package handler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/proto/define"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	logger "github.com/ikun2021/zlog"
	gpush "github.com/luxun9527/gpush/proto"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

const tickRedisMaxLen = 500

type TickHandle struct {
	consumer     pulsar.Consumer
	ticker       *time.Ticker
	batchData    []*matchMq.MatchResult
	svcCtx       *svc.ServiceContext
	msgChan      chan *matchMq.MatchOutput
	currentMsgId pulsar.MessageID
	sendChan     chan *matchMq.MatchResult
	symbolInfo   models.Symbol
	latestMsgId  int64
}

func NewTickHandle(svcCtx *svc.ServiceContext, consumer pulsar.Consumer, symbol models.Symbol) TickHandle {
	ticker := time.NewTicker(time.Minute)
	t := TickHandle{
		consumer:   consumer,
		ticker:     ticker,
		svcCtx:     svcCtx,
		msgChan:    make(chan *matchMq.MatchOutput, 10),
		symbolInfo: symbol,
		sendChan:   make(chan *matchMq.MatchResult, 10),
	}
	go t.start()
	go t.send()
	return t
}

func (t *TickHandle) Handle(message pulsar.Message) {
	var m matchMq.MatchOutput
	if err := proto.Unmarshal(message.Payload(), &m); err != nil {
		logx.Errorw("unmarshal failed", logger.ErrorField(err))
		return
	}
	if t.latestMsgId >= m.MessageId {
		logx.Slowf("recv ignore current msgId %v recv msgId %v", t.latestMsgId, m.MessageId)
		return
	}
	t.msgChan <- &m

	switch r := m.Result.(type) {
	case *matchMq.MatchOutput_MatchResult:
		t.sendChan <- r.MatchResult
		t.currentMsgId = message.ID()
	}
}

func (t *TickHandle) commitBatch() {
	if len(t.batchData) == 0 {
		return
	}
	for {
		if err := t.storeMatchResult(t.svcCtx, t.batchData); err != nil {
			logx.Errorw("store tick failed, retrying...", logger.ErrorField(err))
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	t.batchData = t.batchData[:0]
	logx.Info("tick batch commit success")
}

func (kl *TickHandle) send() {
	for data := range kl.sendChan {
		for _, v := range data.MatchedRecord {
			d := commonWs.Tick{
				Price:        v.Price,
				BaseAmount:   v.BaseAmount,
				QuoteAmount:  v.QuoteAmount,
				TimeStamp:    data.MatchTime,
				TakerIsBuyer: data.TakerIsBuy,
			}
			msg := commonWs.Message[commonWs.Tick]{
				Topic:   commonWs.TickPrefix.WithParam(kl.symbolInfo.Name),
				Payload: d,
			}
			if _, err := kl.svcCtx.WsClient.PushData(context.Background(), &gpush.Data{
				Topic: commonWs.TickPrefix.WithParam(kl.symbolInfo.Name),
				Data:  msg.ToBytes(),
			}); err != nil {
				logx.Errorw("push tick websocket data failed", logger.ErrorField(err), logx.Field("data", msg))
			}
		}
	}
}

func (t TickHandle) start() {
	for {
		select {
		case <-t.ticker.C:
			if len(t.batchData) > 0 {
				t.commitBatch()
			}
		case matchResult := <-t.msgChan:
			switch r := matchResult.Result.(type) {
			case *matchMq.MatchOutput_MatchResult:
				t.batchData = append(t.batchData, r.MatchResult)
			default:
				continue
			}
			if len(t.batchData) >= 100 {
				t.commitBatch()
				t.ticker.Reset(2 * time.Second)
			}
			t.latestMsgId = matchResult.MessageId
		}
	}
}

func (t TickHandle) storeMatchResult(svcCtx *svc.ServiceContext, insertData []*matchMq.MatchResult) error {
	if svcCtx.TickRepo == nil {
		return nil
	}

	docs := make([]*dao.TickDoc, 0, len(insertData)*2)
	redisKey := define.Tick.WithParams(t.symbolInfo.Name)
	ctx := context.Background()

	for _, v := range insertData {
		matchTime := v.MatchTime
		if matchTime > 1e12 {
			matchTime = matchTime / 1e9
		}
		for _, v1 := range v.MatchedRecord {
			makerSide := int32(2)
			if v.TakerIsBuy {
				makerSide = 2
			} else {
				makerSide = 1
			}
			takerSide := int32(1)
			if v.TakerIsBuy {
				takerSide = 1
			} else {
				takerSide = 2
			}

			docs = append(docs,
				&dao.TickDoc{
					PkID:        v1.Maker.Id,
					MatchID:     v.MatchId,
					MatchSubID:  v1.MatchSubId,
					OrderID:     v1.Maker.OrderId,
					UserID:      v1.Maker.Uid,
					Symbol:      v.SymbolName,
					Price:       v1.Price,
					BaseAmount:  v1.BaseAmount,
					QuoteAmount: v1.QuoteAmount,
					Side:        makerSide,
					Role:        1,
					CreatedAt:   matchTime,
				},
				&dao.TickDoc{
					PkID:        v1.Taker.Id,
					MatchID:     v.MatchId,
					MatchSubID:  v1.MatchSubId,
					OrderID:     v1.Taker.OrderId,
					UserID:      v1.Taker.Uid,
					Symbol:      v.SymbolName,
					Price:       v1.Price,
					BaseAmount:  v1.BaseAmount,
					QuoteAmount: v1.QuoteAmount,
					Side:        takerSide,
					Role:        2,
					CreatedAt:   matchTime,
				},
			)

			tickPayload := commonWs.Tick{
				Price:        v1.Price,
				BaseAmount:   v1.BaseAmount,
				QuoteAmount:  v1.QuoteAmount,
				TimeStamp:    matchTime,
				TakerIsBuyer: v.TakerIsBuy,
			}
			if b, err := json.Marshal(tickPayload); err == nil {
				if _, err := svcCtx.RedisClient.Lpush(redisKey, string(b)); err != nil {
					logx.Errorw("cache tick to redis failed", logger.ErrorField(err))
				} else if err := svcCtx.RedisClient.Ltrim(redisKey, 0, tickRedisMaxLen-1); err != nil {
					logx.Errorw("trim tick redis list failed", logger.ErrorField(err))
				}
			}
		}
	}

	if err := svcCtx.TickRepo.InsertMany(ctx, docs); err != nil {
		return err
	}

	if err := t.consumer.AckIDCumulative(t.currentMsgId); err != nil {
		return err
	}
	return nil
}
