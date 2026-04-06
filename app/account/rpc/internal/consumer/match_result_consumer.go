package consumer

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
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
						logx.Errorw("handler message failed", logger.ErrorField(err))
					}
					continue
				}
				switch msg := m.Result.(type) {
				case *matchMq.MatchOutput_MatchResult:
					// 下单成功
				case *matchMq.MatchOutput_CancelResult:
					// 取消订单成功
					Cancel(sc, msg, m.MessageId)
				case *matchMq.MatchOutput_AcceptedResult:
					// 订单被接受
				}
				logx.Debugf("match result %v", &m)

			}

		}(consumer)
	}

}
