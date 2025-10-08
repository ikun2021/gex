package logic

import (
	"context"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/model"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/query"
	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	commonUtils "github.com/ikun2021/gex/common/utils"
	gpush "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
)

// HandleMatchResultLogic 更新订单状态，插入撮合记录
type HandleMatchResultLogic struct {
	svcCtx *svc.ServiceContext
	oc     chan *commonWs.Order
}

func NewHandleMatchResultLogic(svcCtx *svc.ServiceContext) *HandleMatchResultLogic {
	hl := &HandleMatchResultLogic{
		svcCtx: svcCtx,
		oc:     make(chan *commonWs.Order, 5),
	}
	go hl.pushOrderChange()
	return hl
}
func (l HandleMatchResultLogic) pushOrderChange() {
	for order := range l.oc {
		logx.Infow("push ws order data", logx.Field("data", order))
		msg := commonWs.Message[commonWs.Order]{
			Topic:   commonWs.OrderPrefix.WithParam(l.svcCtx.Config.SymbolInfo.SymbolName),
			Payload: *order,
		}
		_, err := l.svcCtx.WsClient.PushData(context.Background(), &gpush.Data{
			Uid:   order.Uid,
			Topic: msg.Topic,
			Data:  msg.ToBytes(),
		})
		if err != nil {
			logx.Errorw("push ws data failed", logger.ErrorField(err))
		}
	}
}

// HandleMatchResult  更新订单状态，插入撮合记录
func (l *HandleMatchResultLogic) HandleMatchResult(result *matchMq.MatchResp_MatchResult, storeConsumedMessageId func() error) error {

	if err := l.svcCtx.Query.Transaction(func(tx *query.Query) error {
		//更新订单状态
		//更新maker订单的状态,一次撮合有多个maker但是只有一个taker
		for _, v := range result.MatchResult.MatchedRecord {
			filledAvgPrice := utils.NewFromStringMaxPrec(v.Maker.FilledAmount).Div(utils.NewFromStringMaxPrec(v.Maker.FilledQty)).String()
			order := &model.EntrustOrder{
				OrderID:        v.Maker.OrderId,
				FilledQty:      v.Maker.FilledQty,
				UnFilledQty:    v.Maker.UnFilledQty,
				FilledAvgPrice: filledAvgPrice,
				FilledAmount:   v.Maker.FilledAmount,
				UnFilledAmount: v.Maker.UnFilledAmount,
				Status:         int32(v.Maker.OrderStatus),
				ID:             v.Maker.Id,
				UserID:         v.Maker.Uid,
			}
			makerOrder := l.svcCtx.Query.EntrustOrder.Table(commonUtils.WithShardingSuffix(order.TableName(), order.UserID))

			if _, err := makerOrder.WithContext(context.Background()).
				Select(makerOrder.FilledQty, makerOrder.UnFilledQty, makerOrder.FilledAvgPrice, makerOrder.FilledAmount, makerOrder.UnFilledAmount, makerOrder.Status).
				Where(makerOrder.ID.Eq(order.ID)).
				Updates(order); err != nil {
				return err
			}
			wsOrder := &commonWs.Order{
				Id:           cast.ToString(order.ID),
				FilledQty:    utils.PrecCut(order.FilledQty, l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()),
				Status:       int8(order.Status),
				FilledAmount: utils.PrecCut(order.FilledAmount, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
				Uid:          cast.ToString(order.UserID),
			}
			l.oc <- wsOrder

		}
		//更新taker订单的状态,只用最后一条数据
		taker := result.MatchResult.MatchedRecord[len(result.MatchResult.MatchedRecord)-1].Taker

		filledAvgPrice := utils.NewFromStringMaxPrec(taker.FilledAmount).Div(utils.NewFromStringMaxPrec(taker.FilledQty)).String()
		order := &model.EntrustOrder{
			OrderID:        taker.OrderId,
			FilledQty:      taker.FilledQty,
			UnFilledQty:    taker.UnFilledQty,
			FilledAvgPrice: filledAvgPrice,
			FilledAmount:   taker.FilledAmount,
			UnFilledAmount: taker.UnFilledAmount,
			Status:         int32(taker.OrderStatus),
			ID:             taker.Id,
			UserID:         taker.Uid,
		}
		wsOrder := &commonWs.Order{
			Id:           cast.ToString(order.ID),
			FilledQty:    utils.PrecCut(order.FilledQty, l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()),
			Status:       int8(order.Status),
			FilledAmount: utils.PrecCut(order.FilledAmount, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
			Uid:          cast.ToString(order.UserID),
		}
		takerOrder := l.svcCtx.Query.EntrustOrder.Table(commonUtils.WithShardingSuffix(order.TableName(), order.UserID))

		if _, err := tx.EntrustOrder.Table(commonUtils.WithShardingSuffix(order.TableName(), order.UserID)).WithContext(context.Background()).
			Select(takerOrder.FilledQty, takerOrder.UnFilledQty, takerOrder.FilledAvgPrice, takerOrder.FilledAmount, takerOrder.UnFilledAmount, takerOrder.Status).
			Where(takerOrder.ID.Eq(order.ID)).
			Updates(order); err != nil {
			return err
		}
		l.oc <- wsOrder

		if err := storeConsumedMessageId(); err != nil {
			return err
		}

		return nil

	}); err != nil {
		return err
	}

	return nil
}

// CancelOrder  取消订单
func (l *HandleMatchResultLogic) CancelOrder(resp *matchMq.MatchResp_Cancel, storeConsumedMessageId func() error) error {

	entrustOrder := l.svcCtx.Query.EntrustOrder.Table(commonUtils.WithShardingSuffix(model.TableNameEntrustOrder, resp.Cancel.Uid))
	if _, err := entrustOrder.WithContext(context.Background()).
		Where(entrustOrder.ID.Eq(resp.Cancel.Id)).
		Update(entrustOrder.Status, int32(enum.OrderStatus_Canceled)); err != nil {
		return err
	}
	wsOrder := &commonWs.Order{
		Id:     cast.ToString(resp.Cancel.Id),
		Status: int8(enum.OrderStatus_Canceled),
		Uid:    cast.ToString(resp.Cancel.Uid),
	}
	if err := storeConsumedMessageId(); err != nil {
		return err
	}
	l.oc <- wsOrder
	return nil
}
