package rediskeys

import "fmt"

const (
	UserActiveOrders RedisKey = "orders:active"
	UserOpenOrders   RedisKey = "open_orders"
)

const maxSlots = 16384

// UserSlotTag 生成 Redis Cluster hash tag，保证同一用户相关 key 落在同一 slot。
func UserSlotTag(uid int64) string {
	return fmt.Sprintf("{%d}", uid%maxSlots)
}

func UserBalanceKey(tag string, uid int64) string {
	return fmt.Sprintf("balance:%s:%d", tag, uid)
}

func UserActiveOrdersKey(tag string, uid int64) string {
	return fmt.Sprintf("%s:%s:%d", UserActiveOrders, tag, uid)
}

// UserOpenOrdersKey 用户活跃订单有序索引，score 为雪花 id，member 为 orderId。
func UserOpenOrdersKey(tag string, uid int64) string {
	return fmt.Sprintf("%s:%s:%d", UserOpenOrders, tag, uid)
}
