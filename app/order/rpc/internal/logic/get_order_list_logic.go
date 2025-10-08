package logic

import (
	"context"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/model"
	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	"github.com/ikun2021/gex/app/order/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	enum "github.com/ikun2021/gex/common/proto/enum"
	"github.com/ikun2021/gex/common/utils"
	commonUtils "github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetOrderListLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetOrderListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetOrderListLogic {
	return &GetOrderListLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetOrderList 获取用户订单列表
func (l *GetOrderListLogic) GetOrderList(in *pb.GetOrderListByUserReq) (*pb.GetOrderListByUserResp, error) {

	entrustOrder := l.svcCtx.Query.EntrustOrder.Table(commonUtils.WithShardingSuffix(model.TableNameEntrustOrder, in.UserId))
	statusCond := make([]int32, 0, len(in.StatusList))
	for _, v := range in.StatusList {
		statusCond = append(statusCond, int32(v))
	}
	idCond := entrustOrder.ID.Lt(in.Id)
	if in.Id == 0 {
		idCond = entrustOrder.ID.Gt(in.Id)

	}
	result, err := entrustOrder.
		WithContext(l.ctx).
		Omit(entrustOrder.UpdatedAt).
		Where(entrustOrder.UserID.Eq(in.UserId), idCond).
		Where(entrustOrder.Status.In(statusCond...)).
		Limit(int(in.PageSize)).
		Order(entrustOrder.ID.Desc()).
		Find()

	count, err := entrustOrder.WithContext(l.ctx).
		Where(entrustOrder.UserID.Eq(in.UserId)).
		Where(entrustOrder.Status.In(statusCond...)).Count()
	if err != nil {
		return nil, errs.ExecSqlFailed
	}
	if err != nil {
		logx.Errorw("GetOrderList query user order list failed", logger.ErrorField(err))
		return nil, errs.ExecSqlFailed
	}
	orders := make([]*pb.Order, 0, len(result))

	for _, v := range result {
		order := &pb.Order{
			Id:             v.ID,
			OrderId:        v.OrderID,
			UserId:         v.UserID,
			SymbolId:       v.SymbolID,
			SymbolName:     v.SymbolName,
			Qty:            utils.PrecCut(v.Qty, l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()),
			Price:          utils.PrecCut(v.Price, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
			Amount:         utils.PrecCut(v.Amount, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
			Side:           enum.Side(v.Side),
			Status:         enum.OrderStatus(v.Status),
			OrderType:      enum.OrderType(v.OrderType),
			FilledQty:      utils.PrecCut(v.FilledQty, l.svcCtx.Config.SymbolInfo.BaseCoinPrec.Load()),
			FilledAmount:   utils.PrecCut(v.FilledAmount, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
			FilledAvgPrice: utils.PrecCut(v.FilledAvgPrice, l.svcCtx.Config.SymbolInfo.QuoteCoinPrec.Load()),
			CreatedAt:      v.CreatedAt,
		}
		orders = append(orders, order)
	}
	return &pb.GetOrderListByUserResp{OrderList: orders, Total: count}, nil
}
