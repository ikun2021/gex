package orderservicelogic

import (
	"context"
	"errors"
	"fmt"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/proto/enum"
	"github.com/ikun2021/gex/common/rediskeys"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

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

// GetOrderList 基于 open_orders 有序索引（score=雪花id）按 id 游标倒序分页，详情从 orders:active 读取。
func (l *GetOrderListLogic) GetOrderList(in *pb.GetOrderListByUserReq) (*pb.GetOrderListByUserResp, error) {
	tag := rediskeys.UserSlotTag(in.UserId)
	openOrdersKey := rediskeys.UserOpenOrdersKey(tag, in.UserId)
	activeOrdersKey := rediskeys.UserActiveOrdersKey(tag, in.UserId)

	statusSet := make(map[enum.OrderStatus]struct{}, len(in.StatusList))
	for _, s := range in.StatusList {
		statusSet[s] = struct{}{}
	}

	pageSize := in.PageSize
	if pageSize <= 0 {
		pageSize = 20
	}

	total, err := l.countMatchingOrders(openOrdersKey, activeOrdersKey, statusSet, in.SymbolName)
	if err != nil {
		return nil, err
	}

	orders, err := l.fetchPage(openOrdersKey, activeOrdersKey, statusSet, in.SymbolName, in.Id, pageSize)
	if err != nil {
		return nil, err
	}

	return &pb.GetOrderListByUserResp{
		OrderList: orders,
		Total:     total,
	}, nil
}

func (l *GetOrderListLogic) countMatchingOrders(openOrdersKey, activeOrdersKey string, statusSet map[enum.OrderStatus]struct{}, symbolName string) (int64, error) {
	if len(statusSet) == 0 && symbolName == "" {
		n, err := l.svcCtx.RedisCli.ZCard(l.ctx, openOrdersKey).Result()
		return n, err
	}

	orderIds, err := l.svcCtx.RedisCli.ZRevRange(l.ctx, openOrdersKey, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("zrevrange order index failed: %w", err)
	}
	return l.countFiltered(orderIds, activeOrdersKey, statusSet, symbolName)
}

func (l *GetOrderListLogic) countFiltered(orderIds []string, activeOrdersKey string, statusSet map[enum.OrderStatus]struct{}, symbolName string) (int64, error) {
	var total int64
	for _, orderId := range orderIds {
		ok, err := l.matchOrder(activeOrdersKey, orderId, statusSet, symbolName)
		if err != nil {
			return 0, err
		}
		if ok {
			total++
		}
	}
	return total, nil
}

func (l *GetOrderListLogic) fetchPage(openOrdersKey, activeOrdersKey string, statusSet map[enum.OrderStatus]struct{}, symbolName string, cursorId, pageSize int64) ([]*pb.Order, error) {
	max := "+inf"
	if cursorId > 0 {
		max = fmt.Sprintf("(%d", cursorId)
	}

	const maxRounds = 10
	orders := make([]*pb.Order, 0, pageSize)
	fetchLimit := pageSize * 2

	for round := 0; round < maxRounds && int64(len(orders)) < pageSize; round++ {
		orderIds, err := l.svcCtx.RedisCli.ZRevRangeByScore(context.Background(), openOrdersKey, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    max,
			Offset: 0,
			Count:  fetchLimit,
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("zrevrangebyscore order index failed: %w", err)
		}
		if len(orderIds) == 0 {
			break
		}

		for _, orderId := range orderIds {
			ok, err := l.matchOrder(activeOrdersKey, orderId, statusSet, symbolName)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			info, err := l.loadOrderInfo(activeOrdersKey, orderId)
			if err != nil {
				return nil, err
			}
			if info == nil {
				continue
			}
			orders = append(orders, orderInfoToOrder(info))
			if int64(len(orders)) >= pageSize {
				break
			}
		}

		if int64(len(orders)) >= pageSize {
			break
		}

		lastScore, err := l.svcCtx.RedisCli.ZScore(l.ctx, openOrdersKey, orderIds[len(orderIds)-1]).Result()
		if err != nil {
			return nil, fmt.Errorf("zscore order index failed: %w", err)
		}
		max = fmt.Sprintf("(%f", lastScore)
	}

	return orders, nil
}

func (l *GetOrderListLogic) matchOrder(activeOrdersKey, orderId string, statusSet map[enum.OrderStatus]struct{}, symbolName string) (bool, error) {
	info, err := l.loadOrderInfo(activeOrdersKey, orderId)
	if err != nil {
		return false, err
	}
	if info == nil {
		return false, nil
	}
	if len(statusSet) > 0 {
		if _, ok := statusSet[info.Status]; !ok {
			return false, nil
		}
	}
	if symbolName != "" && info.SymbolName != symbolName {
		return false, nil
	}
	return true, nil
}

func (l *GetOrderListLogic) loadOrderInfo(activeOrdersKey, orderId string) (*pb.OrderInfo, error) {
	data, err := l.svcCtx.RedisCli.HGet(l.ctx, activeOrdersKey, orderId).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("hget order info failed: %w", err)
	}
	var info pb.OrderInfo
	if err := proto.Unmarshal(data, &info); err != nil {
		l.Logger.Errorf("unmarshal order info failed: %v", err)
		return nil, nil
	}
	return &info, nil
}

func orderInfoToOrder(info *pb.OrderInfo) *pb.Order {
	return &pb.Order{
		Id:                info.Id,
		OrderId:           info.OrderId,
		UserId:            info.UserId,
		SymbolId:          info.SymbolId,
		SymbolName:        info.SymbolName,
		BaseAmount:        info.BaseAmount,
		Price:             info.Price,
		QuoteAmount:       info.QuoteAmount,
		Side:              info.Side,
		Status:            info.Status,
		OrderType:         info.OrderType,
		FilledBaseAmount:  info.FilledBaseAmount,
		FilledQuoteAmount: info.FilledQuoteAmount,
		FilledAvgPrice:    info.FilledAvgPrice,
		CreatedAt:         info.CreatedAt,
		UpdatedAt:         info.UpdatedAt,
	}
}
