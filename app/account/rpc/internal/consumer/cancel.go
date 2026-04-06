package consumer

import (
	"github.com/ikun2021/gex/app/account/rpc/internal/logic"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/zeromicro/go-zero/core/logx"
)

// Cancel 处理从撮合引擎返回的撤单结果
func Cancel(sc *svc.ServiceContext, cancelMatchResult *matchMq.MatchOutput_CancelResult, messageId int64) {
	res := cancelMatchResult.CancelResult

	// 1. 调用逻辑层执行原子资产解冻及快照删除 (Lua 脚本)
	l := logic.NewHandleMatchResultLogic(sc)
	if err := l.HandleCancelOrder(res, messageId, "", func() error { return nil }); err != nil {
		logx.Errorw("HandleCancelOrder atomic execution failed",
			logx.Field("uid", res.Uid),
			logx.Field("orderId", res.OrderId),
			logx.Field("err", err))
		return
	}

	logx.Infow("order canceled successfully (atomic lua)",
		logx.Field("uid", res.Uid),
		logx.Field("orderId", res.OrderId))
}
