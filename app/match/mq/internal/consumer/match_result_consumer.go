package consumer

import (
	"context"
	"github.com/ikun2021/gex/app/match/mq/internal/dao/model"
	"github.com/ikun2021/gex/app/match/mq/internal/logic"
	"github.com/ikun2021/gex/app/match/mq/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

func InitConsumer(sc *svc.ServiceContext) {
	go func() {
		for {
			message, err := sc.MatchConsumer.Receive(context.Background())
			if err != nil {
				logx.Errorw("consumer message match result failed", logger.ErrorField(err))
				continue
			}
			var m matchMq.MatchResp
			if err := proto.Unmarshal(message.Payload(), &m); err != nil {
				logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
				if err := sc.MatchConsumer.Ack(message); err != nil {
					logx.Errorw("consumer message failed", logger.ErrorField(err))
				}
				continue
			}

			m.MessageId = "matchrpc_" + m.MessageId
			logx.Infow("receive match result message", logx.Field("message", &m))
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
				if err := logic.NewStoreMatchResultLogic(sc).StoreMatchResult(r, storeConsumedMessageId); err != nil {
					logx.Severef("consumer match result failed err = %v", err)
				}

				matchData := &model.MatchData{
					MessageID:  message.ID(),
					MatchTime:  r.MatchResult.MatchTime,
					Volume:     utils.NewFromStringMaxPrec(r.MatchResult.Amount).Mul(utils.NewFromStringMaxPrec("2")),
					Amount:     utils.NewFromStringMaxPrec(r.MatchResult.Qty).Mul(utils.NewFromStringMaxPrec("2")),
					StartPrice: utils.NewFromStringMaxPrec(r.MatchResult.BeginPrice),
					EndPrice:   utils.NewFromStringMaxPrec(r.MatchResult.EndPrice),
					Low:        utils.NewFromStringMaxPrec(r.MatchResult.LowPrice),
					High:       utils.NewFromStringMaxPrec(r.MatchResult.HighPrice),
				}
				sc.MatchDataChan <- matchData
			}

			if err := sc.MatchConsumer.Ack(message); err != nil {
				logx.Errorw("ack message failed", logger.ErrorField(err))
			}
		}
	}()

}
