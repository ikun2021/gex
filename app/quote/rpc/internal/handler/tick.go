package handler

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao/quote/model"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/models"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	logger "github.com/ikun2021/zlog"
	gpush "github.com/luxun9527/gpush/proto"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"time"
)

const (
	tradeTableName = "trade_"
)

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
	// --- 数据库写入 (失败重试) ---
	for {
		if err := t.storeMatchResult(t.svcCtx, t.batchData); err != nil {
			logx.Errorw("store match result failed, retrying...", logger.ErrorField(err))
			time.Sleep(3 * time.Second) // 失败退避
			continue
		}
		break
	}

	logx.Info("batch commit success")
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
				logx.Errorw("push kline websocket data failed", logger.ErrorField(err), logx.Field("data", msg))
			}
		}

	}
}
func (t TickHandle) start() {
	// 5. 主循环 (现在是真正的非阻塞 Select)
	for {
		select {
		// 情况 A: 时间到了 -> 强制提交
		case <-t.ticker.C:
			if len(t.batchData) > 0 {
				t.commitBatch()
			}

		// 情况 B: 收到新消息 -> 放入缓冲区
		case matchResult := <-t.msgChan:
			switch r := matchResult.Result.(type) {
			case *matchMq.MatchOutput_MatchResult:
				t.batchData = append(t.batchData, r.MatchResult)
			default:
				continue
			}

			// 数量够了 -> 提交
			if len(t.batchData) >= 100 {
				t.commitBatch()
				// 重置定时器，防止刚提交完马上又触发定时任务，浪费资源
				t.ticker.Reset(2 * time.Second)
			}
			t.latestMsgId = matchResult.MessageId
		}
	}
}
func (t TickHandle) storeMatchResult(svcCtx *svc.ServiceContext, insertData []*matchMq.MatchResult) error {

	var (
		insertGroup = map[string][]*model.Trade{}
	)

	for _, v := range insertData {
		for _, v1 := range v.MatchedRecord {
			//maker
			suffix := cast.ToString(v1.Maker.Uid % 10)
			trades, ok := insertGroup[tradeTableName+suffix]
			var (
				size = int32(1)
			)
			if v.TakerIsBuy {
				size = 2
			}
			if ok {
				//maker
				trades = append(trades, &model.Trade{
					ID:          v1.Maker.Id,
					MatchID:     v.MatchId,
					OrderID:     v1.Maker.OrderId,
					MatchSubID:  v1.MatchSubId,
					UserID:      v1.Maker.Uid,
					Symbol:      v.SymbolName,
					Price:       v1.Price,
					BaseAcmount: v1.BaseAmount,
					QuoteAmount: v1.QuoteAmount,
					Side:        size,
					Role:        1,
					Fee:         "",
					FeeCurrency: "",
					CreatedAt:   v.MatchTime,
				})

			} else {
				insertGroup[tradeTableName+suffix] = []*model.Trade{{
					ID:          v1.Maker.Id,
					MatchID:     v.MatchId,
					OrderID:     v1.Maker.OrderId,
					UserID:      v1.Maker.Uid,
					Symbol:      v.SymbolName,
					Price:       v1.Price,
					BaseAcmount: v1.BaseAmount,
					QuoteAmount: v1.QuoteAmount,
					Side:        size,
					Role:        1,
					Fee:         "",
					FeeCurrency: "",
					CreatedAt:   v.MatchTime,
				}}
			}
			//taker
			size = int32(2)

			if v.TakerIsBuy {
				size = 1
			}
			suffix = cast.ToString(v1.Taker.Uid % 10)
			trades, ok = insertGroup[tradeTableName+suffix]
			if ok {
				trades = append(trades, &model.Trade{
					ID:          v1.Taker.Id,
					MatchID:     v.MatchId,
					OrderID:     v1.Taker.OrderId,
					MatchSubID:  v1.MatchSubId,
					UserID:      v1.Taker.Uid,
					Symbol:      v.SymbolName,
					Price:       v1.Price,
					BaseAcmount: v1.BaseAmount,
					QuoteAmount: v1.QuoteAmount,
					Side:        size,
					Role:        1,
					Fee:         "",
					FeeCurrency: "",
					CreatedAt:   v.MatchTime,
				})

			} else {
				insertGroup[tradeTableName+suffix] = []*model.Trade{{
					ID:          v1.Taker.Id,
					MatchID:     v.MatchId,
					OrderID:     v1.Taker.OrderId,
					MatchSubID:  v1.MatchSubId,
					UserID:      v1.Taker.Uid,
					Symbol:      v.SymbolName,
					Price:       v1.Price,
					BaseAcmount: v1.BaseAmount,
					QuoteAmount: v1.QuoteAmount,
					Side:        size,
					Role:        1,
					Fee:         "",
					FeeCurrency: "",
					CreatedAt:   v.MatchTime,
				}}
			}

		}

	}
	if err := svcCtx.DB.Transaction(func(tx *gorm.DB) error {
		for tableName, data := range insertGroup {
			if err := tx.Table(tableName).CreateInBatches(data, len(data)).Error; err != nil {
				logx.Errorf("insert data failed %v", err)
				return err
			}
		}
		if err := t.consumer.AckIDCumulative(t.currentMsgId); err != nil {
			logx.Errorf("ack message failed %v", err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
