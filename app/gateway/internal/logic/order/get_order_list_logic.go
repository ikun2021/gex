package order

import (
	"context"
	"strconv"

	"github.com/ikun2021/gex/app/account/rpc/client/orderservice"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/enum"

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
	var cursorId int64
	if req.Id != "" {
		cursorId, err = strconv.ParseInt(req.Id, 10, 64)
		if err != nil {
			return nil, errs.WarpMessage(errs.ParamValidateFailed, "invalid id")
		}
	}

	statusList := make([]enum.OrderStatus, 0, len(req.Status))
	for _, s := range req.Status {
		statusList = append(statusList, enum.OrderStatus(s))
	}

	rpcResp, err := l.svcCtx.OrderRpc.GetOrderList(l.ctx, &orderservice.GetOrderListByUserReq{
		UserId:     1,
		StatusList: statusList,
		Id:         cursorId,
		PageSize:   req.PageSize,
		SymbolName: req.SymbolName,
	})
	if err != nil {
		l.Logger.Errorf("get order list failed: %v", err)
		return nil, err
	}

	orderList := make([]*types.OrderInfo, 0, len(rpcResp.OrderList))
	for _, o := range rpcResp.OrderList {
		orderList = append(orderList, &types.OrderInfo{
			Id:                strconv.FormatInt(o.Id, 10),
			OrderId:           o.OrderId,
			UserId:            o.UserId,
			SymbolName:        o.SymbolName,
			Price:             o.Price,
			BaseAmount:        o.BaseAmount,
			QuoteAmount:       o.QuoteAmount,
			Side:              int32(o.Side),
			Status:            int32(o.Status),
			OrderType:         int32(o.OrderType),
			FilledBaseAmount:  o.FilledBaseAmount,
			FilledQuoteAmount: o.FilledQuoteAmount,
			FilledAvgPrice:    o.FilledAvgPrice,
			CreatedAt:         o.CreatedAt,
		})
	}

	return &types.GetOrderListResp{
		OrderList: orderList,
		Total:     rpcResp.Total,
	}, nil
}
