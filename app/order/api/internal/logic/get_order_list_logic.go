package logic

import (
	"context"
	orderpb "github.com/ikun2021/gex/app/order/rpc/pb"
	enum "github.com/ikun2021/gex/common/proto/enum"
	"github.com/spf13/cast"
	"google.golang.org/grpc/metadata"

	"github.com/ikun2021/gex/app/order/api/internal/svc"
	"github.com/ikun2021/gex/app/order/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetOrderListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetOrderListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetOrderListLogic {
	return &GetOrderListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetOrderListLogic) GetOrderList(req *types.GetOrderListReq) (resp *types.GetOrderListResp, err error) {
	uid := l.ctx.Value("uid")

	ctx := metadata.NewIncomingContext(l.ctx, metadata.Pairs("symbol", cast.ToString(req.SymbolName)))

	statusList := make([]enum.OrderStatus, 0, len(req.Status))
	for _, v := range req.Status {
		statusList = append(statusList, enum.OrderStatus(v))
	}
	orderList, err := l.svcCtx.OrderClient.GetOrderList(ctx, &orderpb.GetOrderListByUserReq{
		UserId:     cast.ToInt64(uid),
		StatusList: statusList,
		PageSize:   req.PageSize,
		Id:         cast.ToInt64(req.Id),
	})
	if err != nil {
		return nil, err
	}
	orderInfoList := make([]*types.OrderInfo, 0, len(orderList.OrderList))
	for _, v := range orderList.OrderList {
		orderInfo := &types.OrderInfo{
			Id:             cast.ToString(v.Id),
			OrderId:        v.OrderId,
			UserId:         v.UserId,
			SymbolName:     v.SymbolName,
			Price:          v.Price,
			Qty:            v.Qty,
			Amount:         v.Amount,
			Side:           int32(v.Side),
			Status:         int32(v.Status),
			OrderType:      int32(v.OrderType),
			FilledQty:      v.FilledQty,
			FilledAmount:   v.FilledAmount,
			FilledAvgPrice: v.FilledAvgPrice,
			CreatedAt:      v.CreatedAt,
		}
		orderInfoList = append(orderInfoList, orderInfo)
	}
	resp = &types.GetOrderListResp{OrderList: orderInfoList, Total: orderList.Total}
	return
}
