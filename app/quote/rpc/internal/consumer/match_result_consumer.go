package consumer

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/config"
	"github.com/ikun2021/gex/app/quote/rpc/internal/model"
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

func InitConsumer(sc *svc.ServiceContext) <-chan *model.MatchData {
	md := make(chan *model.MatchData)

	for _, v := range sc.Config.Symbol {
		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Tick,
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
						Volume:     utils.NewFromStringMaxPrec(r.MatchResult.Amount).Mul(utils.NewFromStringMaxPrec("2")),
						Amount:     utils.NewFromStringMaxPrec(r.MatchResult.Qty).Mul(utils.NewFromStringMaxPrec("2")),
						StartPrice: utils.NewFromStringMaxPrec(r.MatchResult.BeginPrice),
						EndPrice:   utils.NewFromStringMaxPrec(r.MatchResult.EndPrice),
						Low:        utils.NewFromStringMaxPrec(r.MatchResult.LowPrice),
						High:       utils.NewFromStringMaxPrec(r.MatchResult.HighPrice),
					}
					md <- matchData
				}

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
						Volume:     utils.NewFromStringMaxPrec(r.MatchResult.Amount).Mul(utils.NewFromStringMaxPrec("2")),
						Amount:     utils.NewFromStringMaxPrec(r.MatchResult.Qty).Mul(utils.NewFromStringMaxPrec("2")),
						StartPrice: utils.NewFromStringMaxPrec(r.MatchResult.BeginPrice),
						EndPrice:   utils.NewFromStringMaxPrec(r.MatchResult.EndPrice),
						Low:        utils.NewFromStringMaxPrec(r.MatchResult.LowPrice),
						High:       utils.NewFromStringMaxPrec(r.MatchResult.HighPrice),
					}
					md <- matchData
				}

			}
		}(v)

		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Kline,
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
						Volume:     utils.NewFromStringMaxPrec(r.MatchResult.Amount).Mul(utils.NewFromStringMaxPrec("2")),
						Amount:     utils.NewFromStringMaxPrec(r.MatchResult.Qty).Mul(utils.NewFromStringMaxPrec("2")),
						StartPrice: utils.NewFromStringMaxPrec(r.MatchResult.BeginPrice),
						EndPrice:   utils.NewFromStringMaxPrec(r.MatchResult.EndPrice),
						Low:        utils.NewFromStringMaxPrec(r.MatchResult.LowPrice),
						High:       utils.NewFromStringMaxPrec(r.MatchResult.HighPrice),
					}
					md <- matchData
				}

			}
		}(v)

		go func(s models.Symbol) {
			consumer, err := sc.PulsarClient.Subscribe(pulsar.ConsumerOptions{
				Topic:            defines.MatchTopicOutputPrefix + s.Name,
				SubscriptionName: config.Depth,
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
						Volume:     utils.NewFromStringMaxPrec(r.MatchResult.Amount).Mul(utils.NewFromStringMaxPrec("2")),
						Amount:     utils.NewFromStringMaxPrec(r.MatchResult.Qty).Mul(utils.NewFromStringMaxPrec("2")),
						StartPrice: utils.NewFromStringMaxPrec(r.MatchResult.BeginPrice),
						EndPrice:   utils.NewFromStringMaxPrec(r.MatchResult.EndPrice),
						Low:        utils.NewFromStringMaxPrec(r.MatchResult.LowPrice),
						High:       utils.NewFromStringMaxPrec(r.MatchResult.HighPrice),
					}
					md <- matchData
				}

			}
		}(v)
	}

	return md
}
