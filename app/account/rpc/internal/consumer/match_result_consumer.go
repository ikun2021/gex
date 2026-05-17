package consumer

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/account/rpc/internal/logic"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

type MatchResultConsumer struct {
	sc *svc.ServiceContext
}

func InitConsumer(sc *svc.ServiceContext) {
	for _, consumer := range sc.MatchConsumerList {
		go func(c pulsar.Consumer) {
			for {
				message, err := c.Receive(context.Background())
				if err != nil {
					logx.Errorw("handler message match result failed", logger.ErrorField(err))
					continue
				}
				var m matchMq.MatchOutput
				if err := proto.Unmarshal(message.Payload(), &m); err != nil {
					logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
					if err := c.Ack(message); err != nil {
						logx.Errorw("ack message failed", logger.ErrorField(err))
					}
					continue
				}
				h := logic.NewHandleMatchResultLogic(sc)
				var handleErr error
				switch msg := m.Result.(type) {
				case *matchMq.MatchOutput_MatchResult:
					logx.Infow("receive match result",
						logx.Field("messageId", m.MessageId),
						logx.Field("symbol", msg.MatchResult.SymbolName),
						logx.Field("matchId", msg.MatchResult.MatchId),
						logx.Field("records", len(msg.MatchResult.MatchedRecord)))
					handleErr = h.HandleMatchResult(msg, func() error {
						return c.Ack(message)
					})
				case *matchMq.MatchOutput_CancelResult:
					Cancel(sc, msg, m.MessageId)
					if err := c.Ack(message); err != nil {
						logx.Errorw("ack cancel message failed", logger.ErrorField(err))
					}
					continue
				case *matchMq.MatchOutput_AcceptedResult:
					handleErr = h.HandleAcceptOrder(msg.AcceptedResult, m.MessageId)
					if handleErr == nil {
						if err := c.Ack(message); err != nil {
							logx.Errorw("ack accept message failed", logger.ErrorField(err))
						}
					}
					continue
				default:
					if err := c.Ack(message); err != nil {
						logx.Errorw("ack unknown message failed", logger.ErrorField(err))
					}
					continue
				}

				if handleErr != nil {
					logx.Errorw("handle match result failed",
						logx.Field("messageId", m.MessageId),
						logger.ErrorField(handleErr))
					continue
				}
				logx.Infow("match result settled",
					logx.Field("messageId", m.MessageId))
			}
		}(consumer)
	}
}
