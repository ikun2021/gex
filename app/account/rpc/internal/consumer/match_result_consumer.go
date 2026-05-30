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
			ctx := context.Background()
			for {
				message, err := c.Receive(ctx)
				if err != nil {
					logx.Errorw("handler message match result failed", logger.ErrorField(err))
					continue
				}
				processMatchOutput(ctx, sc, c, message)
			}
		}(consumer)
	}
}

func processMatchOutput(ctx context.Context, sc *svc.ServiceContext, c pulsar.Consumer, message pulsar.Message) {
	var m matchMq.MatchOutput
	if err := proto.Unmarshal(message.Payload(), &m); err != nil {
		logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
		if err := c.Ack(message); err != nil {
			logx.Errorw("ack message failed", logger.ErrorField(err))
		}
		return
	}
	logx.Debugf("match result %+v", &m)

	dup, err := tryMarkMatchOutputConsumed(ctx, sc.RedisCli, m.MessageId)
	if err != nil {
		logx.Errorw("match output idempotency check failed",
			logx.Field("messageId", m.MessageId),
			logger.ErrorField(err))
		return
	}
	if dup {
		logx.Infow("match output duplicate, skip",
			logx.Field("messageId", m.MessageId))
		if err := c.Ack(message); err != nil {
			logx.Errorw("ack duplicate message failed", logger.ErrorField(err))
		}
		return
	}

	processedOK := false
	defer func() {
		if !processedOK {
			releaseMatchOutputConsumed(ctx, sc.RedisCli, m.MessageId)
		}
	}()

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
			processedOK = true
			return c.Ack(message)
		})
	case *matchMq.MatchOutput_CancelResult:
		handleErr = h.HandleCancelOrder(msg.CancelResult, m.MessageId, "", func() error {
			processedOK = true
			return c.Ack(message)
		})
		if handleErr != nil {
			logx.Errorw("handle cancel result failed",
				logx.Field("messageId", m.MessageId),
				logger.ErrorField(handleErr))
			return
		}
		logx.Infow("cancel result settled", logx.Field("messageId", m.MessageId))
		return
	case *matchMq.MatchOutput_AcceptedResult:
		handleErr = h.HandleAcceptOrder(msg.AcceptedResult, m.MessageId)
		if handleErr != nil {
			logx.Errorw("handle accept result failed",
				logx.Field("messageId", m.MessageId),
				logger.ErrorField(handleErr))
			return
		}
		if err := c.Ack(message); err != nil {
			logx.Errorw("ack accept message failed", logger.ErrorField(err))
			return
		}
		processedOK = true
		logx.Infow("accept result settled", logx.Field("messageId", m.MessageId))
		return
	default:
		if err := c.Ack(message); err != nil {
			logx.Errorw("ack unknown message failed", logger.ErrorField(err))
			return
		}
		processedOK = true
		return
	}

	if handleErr != nil {
		logx.Errorw("handle match result failed",
			logx.Field("messageId", m.MessageId),
			logger.ErrorField(handleErr))
		return
	}
	logx.Infow("match result settled",
		logx.Field("messageId", m.MessageId))
}
