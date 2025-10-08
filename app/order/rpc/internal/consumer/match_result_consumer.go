package consumer

import (
	"context"
	"github.com/ikun2021/gex/app/order/rpc/internal/logic"
	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

func InitConsumer(sc *svc.ServiceContext) {
	matchResultHandler := logic.NewHandleMatchResultLogic(sc)
	go func() {
		for {
			message, err := sc.MatchConsumer.Receive(context.Background())
			if err != nil {
				logx.Errorw("consumer message match result failed", logger.ErrorField(err))
				continue
			}

			var m matchMq.MatchResp
			if err := proto.Unmarshal(message.Payload(), &m); err != nil {
				logx.Sloww("unmarshal match result failed", logger.ErrorField(err))
				if err := sc.MatchConsumer.Ack(message); err != nil {
					logx.Errorw("consumer message failed", logger.ErrorField(err))
				}
				continue
			}
			m.MessageId = "orderrpc_" + m.MessageId
			logx.Infow("consumer message match result success", logx.Field("message", &m))
			//重复提交校验
			existed, err := sc.RedisClient.ExistsCtx(context.Background(), m.MessageId)
			if err != nil {
				logx.Errorw("redis exists failed", logger.ErrorField(err))
				continue
			}
			if existed {
				logx.Sloww("match result message id already exists", logx.Field("message_id", m.MessageId))
				if err := sc.MatchConsumer.Ack(message); err != nil {
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
				if err := matchResultHandler.HandleMatchResult(r, storeConsumedMessageId); err != nil {
					logx.Severef("handle match result failed err=%v data=%v", err, r)
				}

			case *matchMq.MatchResp_Cancel:
				logx.Debugw("receive match cancel data ", logx.Field("data", r))
				if err := matchResultHandler.CancelOrder(r, storeConsumedMessageId); err != nil {
					logx.Severef("[consumer] handle cancel order message failed err=%v data=%v", err, r)
				}
			}
			if err := sc.MatchConsumer.Ack(message); err != nil {
				logx.Errorw("ack message failed", logger.ErrorField(err))
			}

		}

	}()
}
