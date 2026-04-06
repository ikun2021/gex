package logic

import (
	"context"
	"fmt"
	"strings"
	"time"


	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// Lua脚本：原子性处理撮合结果的批量资产结算和订单索引管理
// KEYS: 无 (动态传入)
// ARGV[1]: 记录条数
// ARGV[i*10 + 2]: takerBalanceKey
// ARGV[i*10 + 3]: makerBalanceKey
// ARGV[i*10 + 4]: takerOpenOrdersKey
// ARGV[i*10 + 5]: makerOpenOrdersKey
// ARGV[i*10 + 6]: idempotentKey
// ARGV[i*10 + 7]: takerFrozenField, takerAvailField, takerFrozenDeduct, takerAvailAdd
// ARGV[i*10 + 8]: makerFrozenField, makerAvailField, makerFrozenDeduct, makerAvailAdd
// ARGV[i*10 + 9]: takerOrderId, takerOrderStatus
// ARGV[index + 10]: makerOrderId, makerOrderStatus
// ARGV[index + 11]: idempotentEx
var atomicSettleBatchScript = redis.NewScript(`
local count = tonumber(ARGV[1])
for i=0, count-1 do
    local offset = i * 11 + 1
    local takerBalanceKey = ARGV[offset + 1]
    local makerBalanceKey = ARGV[offset + 2]
    local takerOpenOrdersKey = ARGV[offset + 3]
    local makerOpenOrdersKey = ARGV[offset + 4]
    local idempotentKey = ARGV[offset + 5]
    
    -- 1. 幂等检查
    if redis.call("EXISTS", idempotentKey) == 0 then
        local takerFrozenField = ARGV[offset + 6]
        local takerAvailField = ARGV[offset + 7]
        local takerFrozenDeduct = tonumber(ARGV[offset + 8])
        local takerAvailAdd = tonumber(ARGV[offset + 9])
        
        local makerFrozenField = ARGV[offset + 10]
        local makerAvailField = ARGV[offset + 11]
        local makerFrozenDeduct = tonumber(ARGV[offset + 12])
        local makerAvailAdd = tonumber(ARGV[offset + 13])
        
        local takerOrderId = ARGV[offset + 14]
        local takerOrderStatus = tonumber(ARGV[offset + 15])
        local makerOrderId = ARGV[offset + 16]
        local makerOrderStatus = tonumber(ARGV[offset + 17])
        local idempotentEx = tonumber(ARGV[offset + 18])

        -- 2. 处理 taker 资产
        redis.call("HINCRBY", takerBalanceKey, takerFrozenField, -takerFrozenDeduct)
        redis.call("HINCRBY", takerBalanceKey, takerAvailField, takerAvailAdd)

        -- 3. 处理 maker 资产
        redis.call("HINCRBY", makerBalanceKey, makerFrozenField, -makerFrozenDeduct)
        redis.call("HINCRBY", makerBalanceKey, makerAvailField, makerAvailAdd)

        -- 4. 处理 taker 订单索引
        if takerOrderStatus == 3 then
            redis.call("ZREM", takerOpenOrdersKey, takerOrderId)
        end

        -- 5. 处理 maker 订单索引
        if makerOrderStatus == 3 then
            redis.call("ZREM", makerOpenOrdersKey, makerOrderId)
        end

        -- 6. 标记为已处理
        redis.call("SET", idempotentKey, "1", "EX", idempotentEx)
    end
end
return 1
`)

// Lua脚本：原子性处理订单接收（加入索引）
// KEYS[1]: openOrdersKey (open_orders:{uid}:{symbol})
// KEYS[2]: idempotentKey (match_processed:accept:{message_id})
// ARGV[1]: orderId (订单ID)
// ARGV[2]: score (时间戳)
// ARGV[3]: idempotentEx (过期时间)
var atomicAcceptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
    return 1
end
redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1])
redis.call("SET", KEYS[2], "1", "EX", ARGV[3])
return 1
`)

// Lua脚本：原子性处理订单取消（解冻资产 + 删除活跃快照）
// KEYS[1]: balanceKey (balance:{tag}:uid)
// KEYS[2]: activeOrdersKey (orders:active:{tag}:uid)
// KEYS[3]: idempotentKey (match_processed:cancel:{id}:{tag})
// ARGV[1]: frozenField (冻结字段)
// ARGV[2]: availField (可用字段)
// ARGV[3]: amount (解冻数量, int64)
// ARGV[4]: orderId (订单ID)
// ARGV[5]: idempotentEx (过期时间)
var atomicCancelScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[3]) == 1 then
    return 1
end
-- 1. 解冻资金
redis.call("HINCRBY", KEYS[1], ARGV[1], -tonumber(ARGV[3]))
redis.call("HINCRBY", KEYS[1], ARGV[2], tonumber(ARGV[3]))
-- 2. 删除活跃订单快照
redis.call("HDEL", KEYS[2], ARGV[4])
-- 3. 标记已处理
redis.call("SET", KEYS[3], "1", "EX", ARGV[5])
return 1
`)

// HandleMatchResultLogic 结算
type HandleMatchResultLogic struct {
	svcCtx *svc.ServiceContext
}

// HandleMatchResult  结算，扣减用户资产
func (l *HandleMatchResultLogic) HandleMatchResult(result *matchMq.MatchOutput_MatchResult, storeConsumedMessageId func() error) error {
	if len(result.MatchResult.MatchedRecord) == 0 {
		return nil
	}

	// 解析交易对，获取基础币 and 计价币
	parts := strings.Split(result.MatchResult.SymbolName, "_")
	if len(parts) != 2 {
		logx.Errorw("invalid symbol format", logx.Field("symbol", result.MatchResult.SymbolName))
		return fmt.Errorf("invalid symbol format: %s", result.MatchResult.SymbolName)
	}
	baseCcy, quoteCcy := parts[0], parts[1]

	var batchArgs []interface{}
	batchArgs = append(batchArgs, len(result.MatchResult.MatchedRecord)) // ARGV[1]: count

	// 收集所有结算记录的参数
	for _, record := range result.MatchResult.MatchedRecord {
		args, err := l.getSettleRecordArgs(result.MatchResult, record, baseCcy, quoteCcy, result.MatchResult.SymbolName)
		if err != nil {
			logx.Errorw("get settle record args failed",
				logx.Field("error", err.Error()),
				logx.Field("matchSubId", record.MatchSubId))
			return err
		}
		batchArgs = append(batchArgs, args...)
	}

	// 执行批量原子脚本
	ctx := context.Background()
	_, err := atomicSettleBatchScript.Run(ctx, l.svcCtx.RedisCli, []string{}, batchArgs...).Result()
	if err != nil {
		logx.Errorw("execute batch settle script failed", logx.Field("error", err.Error()))
		return fmt.Errorf("execute batch settle script failed: %w", err)
	}

	// 存储已消费的消息ID（用于幂等性）
	if err := storeConsumedMessageId(); err != nil {
		logx.Errorw("store consumed message id failed", logx.Field("error", err.Error()))
		return err
	}

	logx.Infow("batch settle success", logx.Field("count", len(result.MatchResult.MatchedRecord)))
	return nil
}

// getSettleRecordArgs 准备单条撮合记录的结算参数
func (l *HandleMatchResultLogic) getSettleRecordArgs(matchResult *matchMq.MatchResult, record *matchMq.MatchResult_MatchedRecord, baseCcy, quoteCcy, symbolName string) ([]interface{}, error) {
	taker := record.Taker
	maker := record.Maker

	// 计算 taker 和 maker 的资产变动
	var takerFrozenField, takerAvailField string
	var takerFrozenDeduct, takerAvailAdd int64
	var makerFrozenField, makerAvailField string
	var makerFrozenDeduct, makerAvailAdd int64
	var err error

	if matchResult.TakerIsBuy {
		takerFrozenField = quoteCcy + "_frozen"
		takerAvailField = baseCcy
		takerFrozenDeduct, err = utils.ToDBInteger(quoteCcy, taker.UnFrozenAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker unfrozen amount failed: %w", err)
		}
		takerAvailAdd, err = utils.ToDBInteger(baseCcy, record.BaseAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker base amount failed: %w", err)
		}

		makerFrozenField = baseCcy + "_frozen"
		makerAvailField = quoteCcy
		makerFrozenDeduct, err = utils.ToDBInteger(baseCcy, maker.UnFrozenAmount)
		if err != nil {
			return nil, fmt.Errorf("convert maker unfrozen amount failed: %w", err)
		}
		makerAvailAdd, err = utils.ToDBInteger(quoteCcy, record.QuoteAmount)
		if err != nil {
			return nil, fmt.Errorf("convert maker quote amount failed: %w", err)
		}
	} else {
		takerFrozenField = baseCcy + "_frozen"
		takerAvailField = quoteCcy
		takerFrozenDeduct, err = utils.ToDBInteger(baseCcy, taker.UnFrozenAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker unfrozen amount failed: %w", err)
		}
		takerAvailAdd, err = utils.ToDBInteger(quoteCcy, record.QuoteAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker quote amount failed: %w", err)
		}

		makerFrozenField = quoteCcy + "_frozen"
		makerAvailField = baseCcy
		makerFrozenDeduct, err = utils.ToDBInteger(quoteCcy, maker.UnFrozenAmount)
		if err != nil {
			return nil, fmt.Errorf("convert maker unfrozen amount failed: %w", err)
		}
		makerAvailAdd, err = utils.ToDBInteger(baseCcy, record.BaseAmount)
		if err != nil {
			return nil, fmt.Errorf("convert maker base amount failed: %w", err)
		}
	}

	tagTaker := l.getTag(taker.Uid)
	tagMaker := l.getTag(maker.Uid)
	takerBalanceKey := fmt.Sprintf("balance:%s:%d", tagTaker, taker.Uid)
	makerBalanceKey := fmt.Sprintf("balance:%s:%d", tagMaker, maker.Uid)
	takerOpenOrdersKey := fmt.Sprintf("open_orders:%s:%d:%s", tagTaker, taker.Uid, symbolName)
	makerOpenOrdersKey := fmt.Sprintf("open_orders:%s:%d:%s", tagMaker, maker.Uid, symbolName)
	idempotentKey := fmt.Sprintf("match_processed:%s:%s", record.MatchSubId, tagTaker)

	return []interface{}{
		takerBalanceKey,          // offset + 1
		makerBalanceKey,          // offset + 2
		takerOpenOrdersKey,       // offset + 3
		makerOpenOrdersKey,       // offset + 4
		idempotentKey,            // offset + 5
		takerFrozenField,         // offset + 6
		takerAvailField,          // offset + 7
		takerFrozenDeduct,        // offset + 8
		takerAvailAdd,            // offset + 9
		makerFrozenField,         // offset + 10
		makerAvailField,          // offset + 11
		makerFrozenDeduct,        // offset + 12
		makerAvailAdd,            // offset + 13
		taker.OrderId,            // offset + 14
		int32(taker.OrderStatus), // offset + 15
		maker.OrderId,            // offset + 16
		int32(maker.OrderStatus), // offset + 17
		3600 * 24,                // offset + 18
	}, nil
}

func NewHandleMatchResultLogic(svcCtx *svc.ServiceContext) *HandleMatchResultLogic {
	return &HandleMatchResultLogic{
		svcCtx: svcCtx,
	}
}

func (l *HandleMatchResultLogic) HandleCancelOrder(cancelResp *matchMq.CancelResult, messageId int64, symbolName string, storeConsumedMessageId func() error) error {
	res := cancelResp
	ctx := context.Background()

	// 1. 获取币种名称
	coinName := l.getCoinName(res.CoinId)
	if coinName == "" {
		logx.Errorw("coin not found", logx.Field("coinId", res.CoinId))
		return fmt.Errorf("coin not found: %d", res.CoinId)
	}

	// 2. 准备解冻数量
	unfreezeAmount, err := utils.ToDBInteger(coinName, res.Amount)
	if err != nil {
		return fmt.Errorf("convert amount failed: %w", err)
	}

	// 3. 处理资产解冻及各类索引清理 (原子 Lua 脚本)
	tag := l.getTag(res.Uid)
	balanceKey := fmt.Sprintf("balance:%s:%d", tag, res.Uid)
	activeOrdersKey := fmt.Sprintf("orders:active:%s:%d", tag, res.Uid)
	idempotentKey := fmt.Sprintf("match_processed:cancel:%d:%s", messageId, tag)

	keys := []string{balanceKey, activeOrdersKey, idempotentKey}
	args := []interface{}{
		coinName + "_frozen", // ARGV[1]
		coinName,              // ARGV[2]
		unfreezeAmount,        // ARGV[3]
		res.OrderId,           // ARGV[4]
		3600 * 24,             // ARGV[5]
	}

	_, err = atomicCancelScript.Run(ctx, l.svcCtx.RedisCli, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("execute atomicCancelScript failed: %w", err)
	}

	if err := storeConsumedMessageId(); err != nil {
		return err
	}
	return nil
}

func (l *HandleMatchResultLogic) HandleAcceptOrder(acceptResult *matchMq.AcceptedResult, messageId int64) error {
	res := acceptResult
	ctx := context.Background()

	tag := l.getTag(res.Uid)
	openOrdersKey := fmt.Sprintf("open_orders:%s:%d:%s", tag, res.Uid, res.SymbolName)
	idempotentKey := fmt.Sprintf("match_processed:accept:%d:%s", messageId, tag)

	keys := []string{openOrdersKey, idempotentKey}
	args := []interface{}{
		res.OrderId,
		time.Now().UnixMilli(),
		3600 * 24,
	}

	_, err := atomicAcceptScript.Run(ctx, l.svcCtx.RedisCli, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("execute accept script failed: %w", err)
	}

	return nil
}

func (l *HandleMatchResultLogic) getCoinName(coinId int32) string {
	name := ""
	for _, v := range l.svcCtx.Config.Coin {
		if v.Id == coinId {
			return v.Name
		}
	}
	return name
}

func (l *HandleMatchResultLogic) getTag(uid int64) string {
	const MaxSlots = 16384
	return fmt.Sprintf("{%d}", uid%MaxSlots)
}
