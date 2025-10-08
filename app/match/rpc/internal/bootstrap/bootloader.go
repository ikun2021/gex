package bootstrap

import (
	"context"
	"github.com/ikun2021/gex/app/match/rpc/internal/consumer"
	"github.com/ikun2021/gex/app/match/rpc/internal/engine"
	"github.com/ikun2021/gex/app/match/rpc/internal/svc"
	"github.com/ikun2021/gex/app/order/rpc/orderservice"
	"github.com/ikun2021/gex/common/proto/enum"
	"github.com/ikun2021/gex/common/utils"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/metadata"
)

func Start(sc *svc.ServiceContext) {
	loadOrder(sc)
	consumer.InitMatchConsumer(sc)
}
func loadOrder(sc *svc.ServiceContext) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("symbol", sc.Config.Symbol))
	stream, err := sc.OrderClient.GetOrderAllPendingOrder(ctx, &orderservice.OrderEmpty{})
	if err != nil {
		logx.Severef("call GetOrderAllPendingOrder failed %v", err)
	}
	var maxOrderPrimary int64
	for {
		order, err := stream.Recv()
		if order.Done {
			break
		}
		if err != nil {
			//没有加载完则panic
			logx.Severef("read order from order service failed err = %v", err)
		}
		logx.Infow("init load order", logx.Field("order", order))
		o := &engine.Order{
			Uid:            order.Uid,
			OrderID:        order.OrderId,
			SequenceId:     order.SequenceId,
			CreateTime:     0,
			IsCancel:       false,
			Price:          utils.NewFromStringMaxPrec(order.Price),
			Qty:            utils.NewFromStringMaxPrec(order.Qty),
			OrderType:      order.OrderType,
			Amount:         utils.NewFromStringMaxPrec(order.Amount),
			Side:           order.Side,
			OrderStatus:    enum.OrderStatus_NewCreated,
			UnfilledQty:    utils.NewFromStringMaxPrec(order.UnFilledQty),
			FilledQty:      utils.DecimalZeroMaxPrec,
			UnfilledAmount: utils.NewFromStringMaxPrec(order.UnFilledAmount),
		}
		if order.SequenceId > maxOrderPrimary {
			maxOrderPrimary = order.SequenceId
		}
		sc.MatchEngine.HandleOrder(o)

	}
}
