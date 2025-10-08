package logic

import (
	"context"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/model"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonUtils "github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	"github.com/ikun2021/gex/app/order/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CancelOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCancelOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CancelOrderLogic {
	return &CancelOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// CancelOrder 取消订单
func (l *CancelOrderLogic) CancelOrder(in *pb.CancelOrderReq) (*pb.OrderEmpty, error) {
	//前置检查,检查订单是否是该用户的
	entrustOrder := l.svcCtx.Query.EntrustOrder.Table(commonUtils.WithShardingSuffix(model.TableNameEntrustOrder, in.Uid))

	order, err := entrustOrder.WithContext(l.ctx).
		Select(entrustOrder.UserID, entrustOrder.Status, entrustOrder.Side, entrustOrder.Price, entrustOrder.OrderType).
		Where(entrustOrder.ID.Eq(in.Id)).
		First()
	if err != nil {
		if errors.Is(gorm.ErrRecordNotFound, err) {
			return nil, errs.OrderNotFound
		}
		logx.Errorw("query entrustOrder status failed", logger.ErrorField(err))
		return nil, errs.ExecSqlFailed
	}
	//订单不是该用户id
	if order.UserID != in.Uid {
		return nil, errs.ExecSqlFailed
	}
	//市价单不能手动取消
	if enum.OrderType(order.OrderType) == enum.OrderType_MO {
		return nil, errs.LoOrderCancelFailed
	}
	//订单状态不对
	if enum.OrderStatus(order.Status) != enum.OrderStatus_NewCreated && enum.OrderStatus(order.Status) != enum.OrderStatus_PartFilled {
		return nil, errs.OrderHasResolved
	}

	cancelReq := &matchMq.MatchReq{
		Operate: &matchMq.MatchReq_Cancel{
			Cancel: &matchMq.CancelOperate{
				Id:        in.Id,
				Price:     order.Price,
				Side:      enum.Side(order.Side),
				OrderType: enum.OrderType(order.OrderType),
			},
		},
	}
	data, _ := proto.Marshal(cancelReq)
	if _, err := l.svcCtx.MatchProducer.Send(l.ctx, &pulsar.ProducerMessage{
		Payload: data,
	}); err != nil {
		return nil, errs.PulsarErr
	}
	return &pb.OrderEmpty{}, nil
}
