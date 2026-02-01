package consumer

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	logger "github.com/luxun9527/zlog"
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
					logx.Errorw("consumer message match result failed", logger.ErrorField(err))
					continue
				}
				var m matchMq.MatchInput
				if err := proto.Unmarshal(message.Payload(), &m); err != nil {
					logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
					if err := c.Ack(message); err != nil {
						logx.Errorw("consumer message failed", logger.ErrorField(err))
					}
					continue
				}

				switch r := m.Event.(type) {
				case *matchMq.MatchInput_CreateOrder:

				case *matchMq.MatchInput_CancelOrder:

					//解冻用户资产
				}
				if err := c.Ack(message); err != nil {
					logx.Severef("ack message failed err = %v message =%v", err, message)

				}
			}

		}(consumer)
	}

}
