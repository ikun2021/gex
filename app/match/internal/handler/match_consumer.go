package handler

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/internal/engine"
	"github.com/ikun2021/gex/app/match/internal/svc"
	"github.com/ikun2021/gex/common/defines"
	"github.com/ikun2021/gex/common/models"
	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

var (
	Handlers = map[string]*engine.MatchEngine{}
)

func InitMatchHandler(sc *svc.ServiceContext) {
	ctx := context.Background()

	for _, v := range sc.Config.Symbol {
		go func(symbol models.Symbol) {
			inputTopic := pulsarConfig.Topic{
				Tenant:    pulsarConfig.PublicTenant,
				Namespace: pulsarConfig.GexNamespace,
				Topic:     defines.MatchTopicInputPrefix + v.Name,
			}
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            inputTopic.BuildTopic(),
				SubscriptionName: "match_" + symbol.Name,
			})
			if err != nil {
				logx.Severef("init match handler error:%v", err)
			}
			outputTopic := pulsarConfig.Topic{
				Tenant:    pulsarConfig.PublicTenant,
				Namespace: pulsarConfig.GexNamespace,
				Topic:     defines.MatchTopicOutputPrefix + v.Name,
			}
			logx.Debugf("start match handler output topic=%s", outputTopic)
			producer, err := sc.PulsarClient.CreateProducer(pulsar.ProducerOptions{
				Topic: outputTopic.BuildTopic(),
			})
			if err != nil {
				logx.Severef("init pulsar producer failed %v", err)
			}
			me := engine.NewMatchEngine(symbol, sc.Config, producer, consumer, sc.RedisClient, sc.WsClient)
			me.Start()
			Handlers[symbol.Name] = me
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

				logx.Infof("receive message data %v", &matchReq)
				var inputMessage *engine.InputMessage
				switch event := matchReq.Event.(type) {
				case *matchMq.MatchInput_CreateOrder:
					inputMessage = &engine.InputMessage{
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
					inputMessage = &engine.InputMessage{
						MessageId:   matchReq.MessageId,
						PulsarMsgId: message.ID(),
						OrderPkId:   event.CancelOrder.Id,
						IsCancel:    true,
						Side:        event.CancelOrder.Side,
						OrderType:   event.CancelOrder.OrderType,
						Price:       utils.NewFromString(event.CancelOrder.Price),
					}

				}

				me.HandleOrder(inputMessage)
			}
		}(v)
	}

}
