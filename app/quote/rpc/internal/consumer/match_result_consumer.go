package consumer

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/config"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao/quote/model"
	"github.com/ikun2021/gex/app/quote/rpc/internal/handler"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/defines"
	"github.com/ikun2021/gex/common/models"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

//每个业务X交易对 一个消费组

func InitConsumer(sc *svc.ServiceContext) {

	for _, v := range sc.Config.Symbol {
		//tick
		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Tick,
				Type:             pulsar.Exclusive,
			})
			if err != nil {
				logx.Severef("init consumer failed %v", err)
			}
			// 1. 定义缓冲区 (在循环外)
			tickHandle := handler.NewTickHandle(sc, consumer, v)
			for {
				message, err := consumer.Receive(context.Background())
				if err != nil {
					logx.Errorw("consumer message match result failed", logger.ErrorField(err))
					continue
				}
				tickHandle.Handle(message)

			}
		}(v)

		go func(s models.Symbol) {

			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Ticker,
				Type:             pulsar.Shared,
			})
			if err != nil {
				logx.Severef("init consumer failed %v", err)
			}
			for {
				message, err := consumer.Receive(context.Background())
				if err != nil {
					logx.Errorw("consumer message match result failed", logger.ErrorField(err))
					continue
				}
				var m matchMq.MatchOutput
				if err := proto.Unmarshal(message.Payload(), &m); err != nil {
					logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
					if err := consumer.Ack(message); err != nil {
						logx.Errorw("consumer message failed", logger.ErrorField(err))
					}
					continue
				}
				switch r := m.Result.(type) {
				case *matchMq.MatchOutput_MatchResult:
					logx.Debugw("receive match result data ", logx.Field("data", r))
					matchData := &model.MatchData{
						MessageID:  message.ID(),
						MatchID:    cast.ToInt64(r.MatchResult.MatchId),
						MatchTime:  r.MatchResult.MatchTime / 1e9,
						Volume:     utils.NewFromString(r.MatchResult.Amount).Mul(utils.NewFromString("2")),
						Amount:     utils.NewFromString(r.MatchResult.Qty).Mul(utils.NewFromString("2")),
						StartPrice: utils.NewFromString(r.MatchResult.BeginPrice),
						EndPrice:   utils.NewFromString(r.MatchResult.EndPrice),
						Low:        utils.NewFromString(r.MatchResult.LowPrice),
						High:       utils.NewFromString(r.MatchResult.HighPrice),
					}
					md <- matchData
				}

			}
		}(v)

		//kline
		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Kline,
				Type:             pulsar.Exclusive,
			})
			if err != nil {
				logx.Severef("init consumer failed %v", err)
			}
			klineHandler := handler.NewKlineHandler(sc, consumer, s)
			for {
				message, err := consumer.Receive(context.Background())
				if err != nil {
					logx.Errorw("consumer message match result failed", logger.ErrorField(err))
					continue
				}

				klineHandler.Handle(message)
			}

		}(v)

		//depth
		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Depth,
				Type:             pulsar.Shared,
			})
			if err != nil {
				logx.Severef("init consumer failed %v", err)
			}
			depthHandler := handler.NewDepthHandler(sc, consumer, s)
			for {
				message, err := consumer.Receive(context.Background())
				if err != nil {
					logx.Errorw("consumer message match result failed", logger.ErrorField(err))
					continue
				}
				depthHandler.Handle(message)
			}
		}(v)
	}

}
