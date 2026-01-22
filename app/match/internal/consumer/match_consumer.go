package consumer

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/internal/engine"
	"github.com/ikun2021/gex/app/match/internal/svc"
	"github.com/ikun2021/gex/common/defines"
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

func InitMatchConsumer(sc *svc.ServiceContext) {
	ctx := context.Background()

	for _, v := range sc.Config.Symbol {
		go func(symbol models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:  defines.MatchTopicInputPrefix + symbol.Name,
				Topics: nil,
			})
			if err != nil {
				logx.Severef("init match consumer error:%v", err)
			}
			producer, err := sc.PulsarClient.CreateProducer(pulsar.ProducerOptions{
				Topic: defines.MatchTopicInputPrefix + symbol.Name,
			})
			if err != nil {
				logx.Severef("init pulsar producer failed %v", err)
			}
			me := engine.NewMatchEngine(symbol, sc.Config, producer, sc.RedisClient)

			for {
				message, err := consumer.Receive(ctx)
				if err != nil {
					logx.Errorw("receive message fail", logger.ErrorField(err))
					continue
				}
				//message.ID().String()
				var matchReq matchMq.MatchInput
				if err := proto.Unmarshal(message.Payload(), &matchReq); err != nil {
					logx.Errorw("unmarshal message fail", logger.ErrorField(err))
					continue
				}

				if me.Gte(matchReq.MessageId) {
					logx.Slowf("current msg id %v", matchReq.MessageId)
					continue
				}
				
				logx.Infow("receive message failed", logx.Field("data", &matchReq))
				var order *engine.Order
				switch event := matchReq.Event.(type) {
				case *matchMq.MatchInput_CreateOrder:
					order = &engine.Order{
						MessageId:           matchReq.MessageId,
						PulsarMsgId:         message.ID(),
						Uid:                 event.CreateOrder.Uid,
						OrderID:             event.CreateOrder.OrderId,
						OrderPkId:           event.CreateOrder.SequenceId,
						CreateTime:          0,
						IsCancel:            false,
						Price:               utils.NewFromString(event.CreateOrder.Price),
						BaseAmount:          utils.NewFromString(event.CreateOrder.BaseAmount),
						OrderType:           event.CreateOrder.OrderType,
						QuoteAmount:         utils.NewFromString(event.CreateOrder.QuoteAmount),
						Side:                event.CreateOrder.Side,
						OrderStatus:         enum.OrderStatus_NewCreated,
						UnfilledBaseAmount:  utils.NewFromString(event.CreateOrder.BaseAmount),
						FilledBaseAmount:    utils.DecimalZeroMaxPrec,
						UnfilledQuoteAmount: utils.NewFromString(event.CreateOrder.QuoteAmount),
						FilledQuoteAmount:   utils.DecimalZeroMaxPrec,
					}

				case *matchMq.MatchInput_CancelOrder:
					order = &engine.Order{
						MessageId:   matchReq.MessageId,
						PulsarMsgId: message.ID(),
						OrderPkId:   event.CancelOrder.Id,
						IsCancel:    true,
						Side:        event.CancelOrder.Side,
						OrderType:   event.CancelOrder.OrderType,
						Price:       utils.NewFromString(event.CancelOrder.Price),
					}

				}

				me.HandleOrder(order)
			}
		}(v)
	}

}