package logic

import (
	"context"
	"fmt"
	"strings"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// Lua脚本：原子性处理撮合结果的资产结算和订单索引管理
// KEYS[1]: takerBalanceKey (balance:{taker_uid})
// KEYS[2]: makerBalanceKey (balance:{maker_uid})
// KEYS[3]: takerOpenOrdersKey (open_orders:{taker_uid}:{symbol})
// KEYS[4]: makerOpenOrdersKey (open_orders:{maker_uid}:{symbol})
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
var atomicSettleScript = redis.NewScript(`
local takerBalanceKey = KEYS[1]
local makerBalanceKey = KEYS[2]
local takerOpenOrdersKey = KEYS[3]
local makerOpenOrdersKey = KEYS[4]

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

-- 处理 taker 资产
-- 扣减冻结资产
redis.call("HINCRBY", takerBalanceKey, takerFrozenField, -takerFrozenDeduct)
-- 增加可用资产
redis.call("HINCRBY", takerBalanceKey, takerAvailField, takerAvailAdd)

-- 处理 maker 资产
-- 扣减冻结资产
redis.call("HINCRBY", makerBalanceKey, makerFrozenField, -makerFrozenDeduct)
-- 增加可用资产
redis.call("HINCRBY", makerBalanceKey, makerAvailField, makerAvailAdd)

-- 处理 taker 订单索引
-- 如果订单完全成交 (status=3)，从用户当前委托中移除
if takerOrderStatus == 3 then
    redis.call("ZREM", takerOpenOrdersKey, takerOrderId)
end

-- 处理 maker 订单索引
-- 如果订单完全成交 (status=3)，从用户当前委托中移除
if makerOrderStatus == 3 then
    redis.call("ZREM", makerOpenOrdersKey, makerOrderId)
end

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

	keys := []string{takerBalanceKey, makerBalanceKey, takerOpenOrdersKey, makerOpenOrdersKey}
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

// HandleCancelOrder 取消订单解冻
func (l *HandleMatchResultLogic) HandleCancelOrder(cancelResp *matchMq.MatchOutput_CancelResult, storeConsumedMessageId func() error) error {
	// TODO: 实现取消订单的解冻逻辑
	return nil
}
func (l *HandleMatchResultLogic) HandleAcceptOrder(cancelResp *matchMq.MatchOutput_AcceptedResult, storeConsumedMessageId func() error) error {
	// TODO: 实现取消订单的解冻逻辑
	return nil
}
