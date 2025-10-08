package logic

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/model"
	"github.com/ikun2021/gex/common/errs"
	enum "github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	gpush "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"time"

	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	"github.com/ikun2021/gex/app/order/rpc/pb"
	commonUtils "github.com/ikun2021/gex/common/utils"
	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderLogic {
	return &CreateOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 创建订单
func (l *CreateOrderLogic) CreateOrder(in *pb.CreateOrderReq) (*pb.OrderEmpty, error) {
	if in.OrderType == enum.OrderType_MO {
		if in.Side == enum.Side_Buy {
			in.Price = "0"
			in.Qty = "0"
		} else {
			in.Price = "0"
			in.Amount = "0"
		}
	}
	//订单Id的规则
	//市价单MO
	//限价单LO
	//买1 卖 2
	orderId := "mo"
	if in.OrderType == enum.OrderType_LO {
		orderId = "lo"
	}
	orderId = fmt.Sprintf("%v%v%v", orderId, int32(in.Side), idgen.NextId())

	order := &model.EntrustOrder{
		ID:             idgen.NextId(),
		OrderID:        orderId,
		UserID:         in.UserId,
		SymbolID:       in.SymbolId,
		SymbolName:     in.SymbolName,
		Qty:            in.Qty,
		Price:          in.Price,
		Side:           int32(in.Side),
		Amount:         in.Amount,
		Status:         int32(enum.OrderStatus_NewCreated),
		OrderType:      int32(in.OrderType),
		FilledQty:      "0",
		UnFilledQty:    in.Qty,
		FilledAvgPrice: "0",
		FilledAmount:   "0",
		UnFilledAmount: in.Amount,
		CreatedAt:      time.Now().Unix(),
	}

	barrier, err := dtmgrpc.BarrierFromGrpc(l.ctx)
	if err != nil {
		logx.Errorw("CreateOrder BarrierFromGrpc db failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.DtmErr)
	}
	entrustOrder := l.svcCtx.Query.EntrustOrder
	db, err := entrustOrder.WithContext(l.ctx).UnderlyingDB().DB()
	if err != nil {
		logx.Errorw("CreateOrder get UnderlyingDB failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.ExecSqlFailed)
	}
	toSQL := entrustOrder.WithContext(l.ctx).UnderlyingDB().ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Table(commonUtils.WithShardingSuffix(order.TableName(), order.UserID)).Create(order)
	})
	//测试执行失败的情况

	if err := barrier.CallWithDB(db, func(tx *sql.Tx) error {
		_, err := tx.Exec(toSQL)
		return err
	}); err != nil {
		logx.Errorw("CreateOrder CallWithDB failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.Internal)

	}
	//return nil, errs.CastToDtmError(errs.PulsarErr)
	//构建消息发送
	msg := &matchMq.MatchReq{Operate: &matchMq.MatchReq_NewOrder{
		NewOrder: &matchMq.NewOrderOperate{
			OrderId:    order.OrderID,
			SequenceId: order.ID,
			Uid:        order.UserID,
			Side:       in.Side,
			Price:      in.Price,
			Qty:        in.Qty,
			Amount:     in.Amount,
			OrderType:  in.OrderType,
		},
	}}
	logx.Infow("send message", logx.Field("msg", msg))
	data, _ := proto.Marshal(msg)
	if _, err := l.svcCtx.MatchProducer.Send(l.ctx, &pulsar.ProducerMessage{
		Payload: data,
	}); err != nil {
		logx.Errorw("CreateOrder Send message failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.PulsarErr)
	}
	//发送ws数据
	wsOrder := &commonWs.Order{
		Id:             cast.ToString(order.ID),
		OrderId:        cast.ToString(order.OrderID),
		SymbolName:     order.SymbolName,
		Price:          commonUtils.PrecCut(order.Price, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
		Qty:            commonUtils.PrecCut(order.Qty, l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()),
		Amount:         commonUtils.PrecCut(order.Amount, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
		Side:           int8(order.Side),
		Status:         int8(enum.OrderStatus_NewCreated),
		OrderType:      int8(order.OrderType),
		FilledAmount:   commonUtils.PrecCut(order.FilledAmount, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
		FilledQty:      commonUtils.PrecCut(order.FilledQty, l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()),
		FilledAvgPrice: commonUtils.PrecCut(order.FilledAvgPrice, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
		Uid:            cast.ToString(order.UserID),
		CreatedAt:      order.CreatedAt,
	}
	l.pushWsData(wsOrder)
	return &pb.OrderEmpty{}, nil
}
func (l *CreateOrderLogic) pushWsData(order *commonWs.Order) {
	msg := commonWs.Message[commonWs.Order]{
		Topic:   commonWs.OrderPrefix.WithParam(l.svcCtx.Config.SymbolInfo.SymbolName),
		Payload: *order,
	}
	logx.Infow("push ws data", logx.Field("msg", msg))
	_, err := l.svcCtx.WsClient.PushData(context.Background(), &gpush.Data{
		Uid:   order.Uid,
		Topic: msg.Topic,
		Data:  msg.ToBytes(),
	})
	if err != nil {
		logx.Errorw("push ws data failed", logger.ErrorField(err))
	}
}
