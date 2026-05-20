package consumer

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/config"
	"github.com/ikun2021/gex/app/quote/rpc/internal/handler"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/defines"
	"github.com/ikun2021/gex/common/models"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/logx"
)

// InitConsumer 每个业务 × 交易对一个消费组，分别处理 tick / ticker / kline / depth。
func InitConsumer(sc *svc.ServiceContext) {
	for _, v := range sc.Config.Symbol {
		// tick：成交明细落 MongoDB + Redis 缓存
		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Tick,
				Type:             pulsar.Exclusive,
			})
			if err != nil {
				logx.Severef("init tick consumer failed %v", err)
				return
			}
			tickHandle := handler.NewTickHandle(sc, consumer, s)
			for {
				message, err := consumer.Receive(context.Background())
				if err != nil {
					logx.Errorw("receive match result failed", logger.ErrorField(err))
					continue
				}
				tickHandle.Handle(message)
			}
		}(v)

		// ticker：24h 行情统计
		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Ticker,
				Type:             pulsar.Shared,
			})
			if err != nil {
				logx.Severef("init ticker consumer failed %v", err)
				return
			}
			tickerHandler := handler.NewTickerHandler(sc, consumer, s)
			for {
				message, err := consumer.Receive(context.Background())
				if err != nil {
					logx.Errorw("receive match result failed", logger.ErrorField(err))
					continue
				}
				tickerHandler.Handle(message)
			}
		}(v)

		// kline：历史 K 线落 MongoDB，最新 K 线落 Redis
		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Kline,
				Type:             pulsar.Exclusive,
			})
			if err != nil {
				logx.Severef("init kline consumer failed %v", err)
				return
			}
			klineHandler := handler.NewKlineHandler(sc, consumer, s)
			for {
				message, err := consumer.Receive(context.Background())
				if err != nil {
					logx.Errorw("receive match result failed", logger.ErrorField(err))
					continue
				}
				klineHandler.Handle(message)
			}
		}(v)

	}
}
