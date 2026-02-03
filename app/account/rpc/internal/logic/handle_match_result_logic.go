package logic

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/common/proto/define"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// Lua脚本：原子性处理撮合结果的资产结算和订单索引管理（增加幂等性检查）
// KEYS[1]: takerBalanceKey (balance:{taker_uid})
// KEYS[2]: makerBalanceKey (balance:{maker_uid})
// KEYS[3]: takerOpenOrdersKey (open_orders:{taker_uid}:{symbol})
// KEYS[4]: makerOpenOrdersKey (open_orders:{maker_uid}:{symbol})
// KEYS[5]: idempotentKey (match_processed:{match_sub_id})
// ARGV[1]: takerFrozenField (冻结字段，如 "USDT_frozen" 或 "BTC_frozen")
// ARGV[2]: takerAvailField (可用字段，如 "USDT" 或 "BTC")
// ARGV[3]: takerFrozenDeduct (taker扣减的冻结金额，int64)
// ARGV[4]: takerAvailAdd (taker增加的可用金额，int64)
// ARGV[5]: makerFrozenField (冻结字段)
// ARGV[6]: makerAvailField (可用字段)
// ARGV[7]: makerFrozenDeduct (maker扣减的冻结金额，int64)
// ARGV[8]: makerAvailAdd (maker增加的可用金额，int64)
// ARGV[9]: takerOrderId (taker订单ID)
// ARGV[10]: takerOrderStatus (taker订单状态: 3=全部成交)
// ARGV[11]: makerOrderId (maker订单ID)
// ARGV[12]: makerOrderStatus (maker订单状态: 3=全部成交)
// ARGV[13]: idempotentEx (幂等键过期时间)
var atomicSettleScript = redis.NewScript(`
local takerBalanceKey = KEYS[1]
local makerBalanceKey = KEYS[2]
local takerOpenOrdersKey = KEYS[3]
local makerOpenOrdersKey = KEYS[4]
local idempotentKey = KEYS[5]

-- 1. 幂等检查
if redis.call("EXISTS", idempotentKey) == 1 then
    return 1 -- 已处理，直接返回成功
end

local takerFrozenField = ARGV[1]
local takerAvailField = ARGV[2]
local takerFrozenDeduct = tonumber(ARGV[3])
local takerAvailAdd = tonumber(ARGV[4])

local makerFrozenField = ARGV[5]
local makerAvailField = ARGV[6]
local makerFrozenDeduct = tonumber(ARGV[7])
local makerAvailAdd = tonumber(ARGV[8])

local takerOrderId = ARGV[9]
local takerOrderStatus = tonumber(ARGV[10])
local makerOrderId = ARGV[11]
local makerOrderStatus = tonumber(ARGV[12])
local idempotentEx = tonumber(ARGV[13])

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

// Lua脚本：原子性处理订单取消（解冻资产 + 删除索引）
// KEYS[1]: balanceKey (balance:{uid})
// KEYS[2]: openOrdersKey (open_orders:{uid}:{symbol})
// KEYS[3]: idempotentKey (match_processed:cancel:{message_id})
// ARGV[1]: frozenField (冻结字段)
// ARGV[2]: availField (可用字段)
// ARGV[3]: amount (解冻数量, int64)
// ARGV[4]: orderId (订单ID)
// ARGV[5]: idempotentEx (过期时间)
var atomicCancelScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[3]) == 1 then
    return 1
end
-- 1. 解冻
redis.call("HINCRBY", KEYS[1], ARGV[1], -tonumber(ARGV[3]))
redis.call("HINCRBY", KEYS[1], ARGV[2], tonumber(ARGV[3]))
-- 2. 删除索引
redis.call("ZREM", KEYS[2], ARGV[4])
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

	// 解析交易对，获取基础币和计价币
	parts := strings.Split(result.MatchResult.SymbolName, "_")
	if len(parts) != 2 {
		logx.Errorw("invalid symbol format", logx.Field("symbol", result.MatchResult.SymbolName))
		return fmt.Errorf("invalid symbol format: %s", result.MatchResult.SymbolName)
	}
	baseCcy, quoteCcy := parts[0], parts[1]

	// 处理每一条撮合记录
	for _, record := range result.MatchResult.MatchedRecord {
		if err := l.settleMatchedRecord(result.MatchResult, record, baseCcy, quoteCcy, result.MatchResult.SymbolName); err != nil {
			logx.Errorw("settle matched record failed",
				logx.Field("error", err.Error()),
				logx.Field("matchSubId", record.MatchSubId),
				logx.Field("takerOrderId", record.Taker.OrderId),
				logx.Field("makerOrderId", record.Maker.OrderId))
			return err
		}
	}

	// 存储已消费的消息ID（用于幂等性）
	if err := storeConsumedMessageId(); err != nil {
		logx.Errorw("store consumed message id failed", logx.Field("error", err.Error()))
		return err
	}

	return nil
}

// settleMatchedRecord 处理单条撮合记录的资产结算
func (l *HandleMatchResultLogic) settleMatchedRecord(matchResult *matchMq.MatchResult, record *matchMq.MatchResult_MatchedRecord, baseCcy, quoteCcy, symbolName string) error {
	ctx := context.Background()
	taker := record.Taker
	maker := record.Maker

	// 计算 taker 和 maker 的资产变动
	var takerFrozenField, takerAvailField string
	var takerFrozenDeduct, takerAvailAdd int64
	var makerFrozenField, makerAvailField string
	var makerFrozenDeduct, makerAvailAdd int64
	var err error

	// Taker 的资产变动
	// 如果 taker 是买单，扣减冻结的计价币(quote)，增加可用的基础币(base)
	// 如果 taker 是卖单，扣减冻结的基础币(base)，增加可用的计价币(quote)
	if matchResult.TakerIsBuy {
		// Taker 买入：扣减冻结的 USDT，增加可用的 BTC
		takerFrozenField = quoteCcy + "_frozen"
		takerAvailField = baseCcy
		takerFrozenDeduct, err = utils.ToDBInteger(quoteCcy, taker.UnFrozenAmount)
		if err != nil {
			return fmt.Errorf("convert taker unfrozen amount failed: %w", err)
		}
		takerAvailAdd, err = utils.ToDBInteger(baseCcy, record.BaseAmount)
		if err != nil {
			return fmt.Errorf("convert taker base amount failed: %w", err)
		}

		// Maker 卖出：扣减冻结的 BTC，增加可用的 USDT
		makerFrozenField = baseCcy + "_frozen"
		makerAvailField = quoteCcy
		makerFrozenDeduct, err = utils.ToDBInteger(baseCcy, maker.UnFrozenAmount)
		if err != nil {
			return fmt.Errorf("convert maker unfrozen amount failed: %w", err)
		}
		makerAvailAdd, err = utils.ToDBInteger(quoteCcy, record.QuoteAmount)
		if err != nil {
			return fmt.Errorf("convert maker quote amount failed: %w", err)
		}
	} else {
		// Taker 卖出：扣减冻结的 BTC，增加可用的 USDT
		takerFrozenField = baseCcy + "_frozen"
		takerAvailField = quoteCcy
		takerFrozenDeduct, err = utils.ToDBInteger(baseCcy, taker.UnFrozenAmount)
		if err != nil {
			return fmt.Errorf("convert taker unfrozen amount failed: %w", err)
		}
		takerAvailAdd, err = utils.ToDBInteger(quoteCcy, record.QuoteAmount)
		if err != nil {
			return fmt.Errorf("convert taker quote amount failed: %w", err)
		}

		// Maker 买入：扣减冻结的 USDT，增加可用的 BTC
		makerFrozenField = quoteCcy + "_frozen"
		makerAvailField = baseCcy
		makerFrozenDeduct, err = utils.ToDBInteger(quoteCcy, maker.UnFrozenAmount)
		if err != nil {
			return fmt.Errorf("convert maker unfrozen amount failed: %w", err)
		}
		makerAvailAdd, err = utils.ToDBInteger(baseCcy, record.BaseAmount)
		if err != nil {
			return fmt.Errorf("convert maker base amount failed: %w", err)
		}
	}

	// 准备 Lua 脚本参数
	takerBalanceKey := fmt.Sprintf("balance:%d", taker.Uid)
	makerBalanceKey := fmt.Sprintf("balance:%d", maker.Uid)
	takerOpenOrdersKey := fmt.Sprintf("open_orders:%d:%s", taker.Uid, symbolName)
	makerOpenOrdersKey := fmt.Sprintf("open_orders:%d:%s", maker.Uid, symbolName)
	idempotentKey := fmt.Sprintf("match_processed:%s", record.MatchSubId)

	keys := []string{takerBalanceKey, makerBalanceKey, takerOpenOrdersKey, makerOpenOrdersKey, idempotentKey}
	args := []interface{}{
		takerFrozenField,         // ARGV[1]
		takerAvailField,          // ARGV[2]
		takerFrozenDeduct,        // ARGV[3]
		takerAvailAdd,            // ARGV[4]
		makerFrozenField,         // ARGV[5]
		makerAvailField,          // ARGV[6]
		makerFrozenDeduct,        // ARGV[7]
		makerAvailAdd,            // ARGV[8]
		taker.OrderId,            // ARGV[9]
		int32(taker.OrderStatus), // ARGV[10]
		maker.OrderId,            // ARGV[11]
		int32(maker.OrderStatus), // ARGV[12]
		3600 * 24,                // ARGV[13] 幂等记录 24 小时过期
	}

	// 执行原子脚本
	_, err = atomicSettleScript.Run(ctx, l.svcCtx.RedisCli, keys, args...).Result()
	if err != nil {
		logx.Errorw("execute settle script failed",
			logx.Field("error", err.Error()),
			logx.Field("matchSubId", record.MatchSubId))
		return fmt.Errorf("execute settle script failed: %w", err)
	}

	logx.Infow("settle matched record success",
		logx.Field("matchSubId", record.MatchSubId),
		logx.Field("takerUid", taker.Uid),
		logx.Field("makerUid", maker.Uid),
		logx.Field("baseAmount", record.BaseAmount),
		logx.Field("quoteAmount", record.QuoteAmount))

	return nil
}

func NewHandleMatchResultLogic(svcCtx *svc.ServiceContext) *HandleMatchResultLogic {
	return &HandleMatchResultLogic{
		svcCtx: svcCtx,
	}
}

func (l *HandleMatchResultLogic) HandleCancelOrder(cancelResp *matchMq.CancelResult, messageId int64, storeConsumedMessageId func() error) error {
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

	// 3. 构建订单 ID
	// 策略：限价单(2) + 方向 + ID
	orderId := fmt.Sprintf("%d%d%d", 2, int32(res.Side), res.Id)

	// 4. 处理资产解冻
	balanceKey := fmt.Sprintf("balance:%d", res.Uid)
	idempotentKey := fmt.Sprintf("match_processed:cancel:%d", messageId)

	// TODO: 目前 CancelResult 缺少 symbol_name，暂时无法删除分区索引 open_orders:{uid}:{symbol}
	// 后续如果协议更新，可以将 dummy 替换为真实 key。
	keys := []string{balanceKey, "open_orders_dummy", idempotentKey}
	args := []interface{}{
		coinName + "_frozen",
		coinName,
		unfreezeAmount,
		orderId,
		3600 * 24,
	}

	_, err = atomicCancelScript.Run(ctx, l.svcCtx.RedisCli, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("execute cancel script failed: %w", err)
	}

	if err := storeConsumedMessageId(); err != nil {
		return err
	}
	return nil
}

func (l *HandleMatchResultLogic) HandleAcceptOrder(acceptResult *matchMq.AcceptedResult, messageId int64, storeConsumedMessageId func() error) error {
	res := acceptResult
	ctx := context.Background()

	openOrdersKey := fmt.Sprintf("open_orders:%d:%s", res.Uid, res.SymbolName)
	idempotentKey := fmt.Sprintf("match_processed:accept:%d", messageId)

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

	if err := storeConsumedMessageId(); err != nil {
		return err
	}
	return nil
}

func (l *HandleMatchResultLogic) getCoinName(coinId int32) string {
	name := ""
	l.svcCtx.Coins.Range(func(key, value any) bool {
		coin := value.(*define.CoinInfo)
		if coin.CoinID == coinId {
			name = coin.CoinName
			return false
		}
		return true
	})
	return name
}
