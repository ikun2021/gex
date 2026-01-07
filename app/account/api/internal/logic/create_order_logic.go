package logic

import (
	"context"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"strings"
	"time"

	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/app/account/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderLogic {
	return &CreateOrderLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateOrderLogic) CreateOrder(req *types.CreateOrderReq) (resp *types.Empty, err error) {

	// ==========================================
	// 到这里，资金已成功锁定。必须保证后续逻辑完成。
	// ==========================================

	// 4. 生成订单 ID
	orderID := s.node.Generate().Int64()
	now := time.Now().UnixMilli()

	// 5. 组装订单对象
	order := Order{
		ID:        orderID,
		UID:       uid,
		Symbol:    symbol,
		Side:      side,
		Price:     priceStr,
		Amount:    amountStr,
		Status:    "NEW",
		CreatedAt: now,
	}
	orderBytes, _ := json.Marshal(order)

	// 6. Redis Pipeline 写入 (减少 RTT)
	pipe := s.rdb.Pipeline()

	// 6.1 存订单详情 (OrderHash)
	// 使用 HSET 存 JSON，也可以拆字段存，JSON 更省 Key 空间
	orderKey := fmt.Sprintf("order:%d", orderID)
	pipe.HSet(ctx, orderKey, "detail", orderBytes)
	pipe.HSet(ctx, orderKey, "status", "NEW")  // 单独存状态方便 Lua 更新
	pipe.Expire(ctx, orderKey, 7*24*time.Hour) // 7天过期

	// 6.2 存当前委托索引 (ZSet)
	// 用于前端查询 "Open Orders"
	openOrderKey := fmt.Sprintf("open_orders:%d:%s", uid, symbol)
	pipe.ZAdd(ctx, openOrderKey, redis.Z{
		Score:  float64(now),
		Member: orderID,
	})

	_, err = pipe.Exec(ctx)
	if err != nil {
		// 严重：钱扣了单没下成功。
		// 生产环境策略：记录 ErrorLog -> 触发报警 -> 后台补偿脚本(Reconciler)根据 balance 日志回滚
		// 这里简单 Panic 或者 Return Error
		return 0, fmt.Errorf("critical error: money frozen but order failed: %v", err)
	}

	// 7. 发送 MQ (模拟)
	// 真实的 MQ 发送通常是异步的，或者保证 At-Least-Once
	go func() {
		fmt.Printf(">> MQ Produce: Topic=order_input Key=%s Payload=%s\n", symbol, string(orderBytes))
	}()

	return orderID, nil

	return
}

// 脚本逻辑：
// 1. 检查余额 (转数字比较)
// 2. 扣减可用 (HINCRBY 负数)
// 3. 增加冻结 (HINCRBY 正数)
