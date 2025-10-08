package consumer

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/account/rpc/internal/logic"
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
				var m matchMq.MatchResp
				if err := proto.Unmarshal(message.Payload(), &m); err != nil {
					logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
					if err := c.Ack(message); err != nil {
						logx.Errorw("consumer message failed", logger.ErrorField(err))
					}
					continue
				}
				//大家用的都是相同的redis需要加前缀区分
				m.MessageId = "accountrpc_" + m.MessageId
				logx.Infow("receive message match result", logx.Field("message", &m))
				//重复提交校验
				existed, err := sc.RedisClient.ExistsCtx(context.Background(), m.MessageId)
				if err != nil {
					logx.Errorw("redis exists failed", logger.ErrorField(err))
					continue
				}
				if existed {
					logx.Sloww("match result message id already exists", logx.Field("message", &m))
					if err := c.Ack(message); err != nil {
						logx.Errorw("ack message failed", logger.ErrorField(err))
					}
					continue
				}
				storeConsumedMessageId := func() error {
					//保存7天
					if err := sc.RedisClient.SetexCtx(context.Background(), m.MessageId, "", 86400*7); err != nil {
						logx.Errorw("redis setex failed", logger.ErrorField(err))
						return err
					}
					return nil
				}
				switch r := m.Resp.(type) {
				case *matchMq.MatchResp_MatchResult:

					logx.Debugw("receive match result data ", logx.Field("data", r))
					//修改订单状态，插入到成交表中，修改用户的资产
					if err := logic.NewHandleMatchResultLogic(sc).HandleMatchResult(r, storeConsumedMessageId); err != nil {
						logx.Severef("[consumer]handle match result failed err=%v data=%v", err, r)
					}
				case *matchMq.MatchResp_Cancel:

					logx.Debugw("receive match cancel data ", logx.Field("data", r))
					if err := logic.NewHandleMatchResultLogic(sc).HandleCancelOrder(r, storeConsumedMessageId); err != nil {
						logx.Severef("[consumer]  match result cancel order failed err=%v data=%v", err, r)
					}
					//解冻用户资产
				}
				if err := c.Ack(message); err != nil {
					logx.Severef("ack message failed err = %v message =%v", err, message)

				}
			}

		}(consumer)
	}

}
