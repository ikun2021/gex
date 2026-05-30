package logic

import (
	"context"
	"fmt"
	"strings"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/rediskeys"
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
// ARGV[i*10 + 6]: takerFrozenField, takerAvailField, takerFrozenDeduct, takerAvailAdd
// ARGV[i*10 + 7]: makerFrozenField, makerAvailField, makerFrozenDeduct, makerAvailAdd
// ARGV[i*10 + 8]: takerOrderId, takerOrderStatus
// ARGV[index + 9]: makerOrderId, makerOrderStatus
// 每条记录 stride=18 个 ARGV 槽位
var atomicSettleBatchScript = redis.NewScript(`
local count = tonumber(ARGV[1])
local stride = 18
for i=0, count-1 do
    local offset = i * stride + 1
    local takerBalanceKey = ARGV[offset + 1]
    local makerBalanceKey = ARGV[offset + 2]
    local takerOpenOrdersKey = ARGV[offset + 3]
    local makerOpenOrdersKey = ARGV[offset + 4]
    local takerFrozenField = ARGV[offset + 5]
    local takerAvailField = ARGV[offset + 6]
    local takerFrozenDeduct = tonumber(ARGV[offset + 7])
    local takerAvailAdd = tonumber(ARGV[offset + 8])
    local makerFrozenField = ARGV[offset + 9]
    local makerAvailField = ARGV[offset + 10]
    local makerFrozenDeduct = tonumber(ARGV[offset + 11])
    local makerAvailAdd = tonumber(ARGV[offset + 12])
    local takerOrderId = ARGV[offset + 13]
    local takerOrderStatus = tonumber(ARGV[offset + 14])
    local makerOrderId = ARGV[offset + 15]
    local makerOrderStatus = tonumber(ARGV[offset + 16])
    local takerActiveOrdersKey = ARGV[offset + 17]
    local makerActiveOrdersKey = ARGV[offset + 18]

    redis.call("HINCRBY", takerBalanceKey, takerFrozenField, -takerFrozenDeduct)
    redis.call("HINCRBY", takerBalanceKey, takerAvailField, takerAvailAdd)
    redis.call("HINCRBY", makerBalanceKey, makerFrozenField, -makerFrozenDeduct)
    redis.call("HINCRBY", makerBalanceKey, makerAvailField, makerAvailAdd)

    if takerOrderStatus == 3 or takerOrderStatus == 4 then
        redis.call("ZREM", takerOpenOrdersKey, takerOrderId)
        redis.call("HDEL", takerActiveOrdersKey, takerOrderId)
    end
    if makerOrderStatus == 3 or makerOrderStatus == 4 then
        redis.call("ZREM", makerOpenOrdersKey, makerOrderId)
        redis.call("HDEL", makerActiveOrdersKey, makerOrderId)
    end
end
return 1
`)

// Lua脚本：原子性处理订单取消（解冻资产 + 删除活跃快照 + 移除有序索引）
// KEYS[1]: balanceKey
// KEYS[2]: activeOrdersKey
// KEYS[3]: openOrdersKey
var atomicCancelScript = redis.NewScript(`
redis.call("HINCRBY", KEYS[1], ARGV[1], -tonumber(ARGV[3]))
redis.call("HINCRBY", KEYS[1], ARGV[2], tonumber(ARGV[3]))
redis.call("HDEL", KEYS[2], ARGV[4])
redis.call("ZREM", KEYS[3], ARGV[4])
return 1
`)

// HandleMatchResultLogic 结算
type HandleMatchResultLogic struct {
	svcCtx *svc.ServiceContext
}

// HandleMatchResult  结算，扣减用户资产
func (l *HandleMatchResultLogic) HandleMatchResult(result *matchMq.MatchOutput_MatchResult, storeConsumedMessageId func() error) error {
	if len(result.MatchResult.MatchedRecord) == 0 {
		logx.Infow("skip empty match result")
		return nil
	}

	match := result.MatchResult
	logx.Infow("handle match result start",
		logx.Field("matchId", match.MatchId),
		logx.Field("symbol", match.SymbolName),
		logx.Field("recordCount", len(match.MatchedRecord)),
		logx.Field("takerIsBuy", match.TakerIsBuy))

	// 解析交易对，获取基础币 and 计价币
	parts := strings.Split(match.SymbolName, "_")
	if len(parts) != 2 {
		logx.Errorw("invalid symbol format", logx.Field("symbol", match.SymbolName))
		return fmt.Errorf("invalid symbol format: %s", match.SymbolName)
	}
	baseCcy, quoteCcy := parts[0], parts[1]

	ctx := context.Background()
	matchTrades := collectMatchTradesFromMatch(match)
	terminalDocs, err := l.collectTerminalOrdersFromMatch(ctx, match)
	if err != nil {
		logx.Errorw("collect terminal orders before settle failed",
			logx.Field("matchId", match.MatchId),
			logx.Field("error", err.Error()))
		return err
	}
	logx.Infow("prepare settle archive data",
		logx.Field("matchId", match.MatchId),
		logx.Field("matchTradeCount", len(matchTrades)),
		logx.Field("terminalOrderCount", len(terminalDocs)))

	var batchArgs []interface{}
	batchArgs = append(batchArgs, len(match.MatchedRecord)) // ARGV[1]: count

	// 收集所有结算记录的参数
	for _, record := range match.MatchedRecord {
		args, err := l.getSettleRecordArgs(match, record, baseCcy, quoteCcy, match.SymbolName)
		if err != nil {
			logx.Errorw("get settle record args failed",
				logx.Field("error", err.Error()),
				logx.Field("matchSubId", record.MatchSubId))
			return err
		}
		batchArgs = append(batchArgs, args...)
	}

	// 执行批量原子脚本
	logx.Infow("execute redis settle lua",
		logx.Field("matchId", match.MatchId),
		logx.Field("settleRecordCount", len(match.MatchedRecord)))
	_, err = atomicSettleBatchScript.Run(ctx, l.svcCtx.RedisCli, []string{}, batchArgs...).Result()
	if err != nil {
		logx.Errorw("execute batch settle script failed",
			logx.Field("matchId", match.MatchId),
			logx.Field("error", err.Error()))
		return fmt.Errorf("execute batch settle script failed: %w", err)
	}
	logx.Infow("redis settle lua success", logx.Field("matchId", match.MatchId))

	if err := persistSettleArchiveTx(ctx, l.svcCtx, matchTrades, terminalDocs); err != nil {
		logx.Errorw("persist settle archive failed",
			logx.Field("matchId", match.MatchId),
			logx.Field("error", err.Error()))
		return fmt.Errorf("persist settle archive to mongodb failed: %w", err)
	}

	if err := storeConsumedMessageId(); err != nil {
		logx.Errorw("ack match result message failed",
			logx.Field("matchId", match.MatchId),
			logx.Field("error", err.Error()))
		return err
	}

	logx.Infow("handle match result success",
		logx.Field("matchId", match.MatchId),
		logx.Field("symbol", match.SymbolName),
		logx.Field("settleRecordCount", len(match.MatchedRecord)),
		logx.Field("matchTradeCount", len(matchTrades)),
		logx.Field("terminalOrderCount", len(terminalDocs)))
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
		takerFrozenDeduct, err = l.frozenDeductInt(quoteCcy, taker.UnFrozenAmount, record.QuoteAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker unfrozen amount failed: %w", err)
		}
		takerAvailAdd, err = utils.ToDBInteger(baseCcy, record.BaseAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker base amount failed: %w", err)
		}

		makerFrozenField = baseCcy + "_frozen"
		makerAvailField = quoteCcy
		makerFrozenDeduct, err = l.frozenDeductInt(baseCcy, maker.UnFrozenAmount, record.BaseAmount)
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
		takerFrozenDeduct, err = l.frozenDeductInt(baseCcy, taker.UnFrozenAmount, record.BaseAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker unfrozen amount failed: %w", err)
		}
		takerAvailAdd, err = utils.ToDBInteger(quoteCcy, record.QuoteAmount)
		if err != nil {
			return nil, fmt.Errorf("convert taker quote amount failed: %w", err)
		}

		makerFrozenField = quoteCcy + "_frozen"
		makerAvailField = baseCcy
		makerFrozenDeduct, err = l.frozenDeductInt(quoteCcy, maker.UnFrozenAmount, record.QuoteAmount)
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
	takerBalanceKey := rediskeys.UserBalanceKey(tagTaker, taker.Uid)
	makerBalanceKey := rediskeys.UserBalanceKey(tagMaker, maker.Uid)
	takerOpenOrdersKey := rediskeys.UserOpenOrdersKey(tagTaker, taker.Uid)
	makerOpenOrdersKey := rediskeys.UserOpenOrdersKey(tagMaker, maker.Uid)
	takerActiveOrdersKey := rediskeys.UserActiveOrdersKey(tagTaker, taker.Uid)
	makerActiveOrdersKey := rediskeys.UserActiveOrdersKey(tagMaker, maker.Uid)

	return []interface{}{
		takerBalanceKey,
		makerBalanceKey,
		takerOpenOrdersKey,
		makerOpenOrdersKey,
		takerFrozenField,
		takerAvailField,
		takerFrozenDeduct,
		takerAvailAdd,
		makerFrozenField,
		makerAvailField,
		makerFrozenDeduct,
		makerAvailAdd,
		taker.OrderId,
		int32(taker.OrderStatus),
		maker.OrderId,
		int32(maker.OrderStatus),
		takerActiveOrdersKey,
		makerActiveOrdersKey,
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

	logx.Infow("handle cancel result start",
		logx.Field("messageId", messageId),
		logx.Field("orderId", res.OrderId),
		logx.Field("uid", res.Uid),
		logx.Field("symbol", symbolName))

	// 1. 获取币种名称
	coinName := l.getCoinName(res.CoinId)
	if coinName == "" {
		logx.Errorw("coin not found", logx.Field("coinId", res.CoinId))
		return fmt.Errorf("coin not found: %d", res.CoinId)
	}

	// 2. 准备解冻数量
	unfreezeAmount, err := utils.ToDBInteger(coinName, res.Amount)
	if err != nil {
		logx.Errorw("convert cancel unfreeze amount failed",
			logx.Field("orderId", res.OrderId),
			logx.Field("amount", res.Amount),
			logx.Field("error", err.Error()))
		return fmt.Errorf("convert amount failed: %w", err)
	}

	activeInfo, err := l.loadActiveOrderInfo(ctx, res.Uid, res.OrderId)
	if err != nil {
		logx.Errorw("load active order before cancel failed",
			logx.Field("orderId", res.OrderId),
			logx.Field("uid", res.Uid),
			logx.Field("error", err.Error()))
		return fmt.Errorf("load active order before cancel: %w", err)
	}
	if activeInfo == nil {
		logx.Infow("active order snapshot not found before cancel",
			logx.Field("orderId", res.OrderId),
			logx.Field("uid", res.Uid))
	}
	cancelDoc := buildOrderFinalFromCancel(activeInfo, res, finishReasonCancel)

	// 3. 处理资产解冻及各类索引清理 (原子 Lua 脚本)
	tag := l.getTag(res.Uid)
	balanceKey := rediskeys.UserBalanceKey(tag, res.Uid)
	activeOrdersKey := rediskeys.UserActiveOrdersKey(tag, res.Uid)
	openOrdersKey := rediskeys.UserOpenOrdersKey(tag, res.Uid)

	keys := []string{balanceKey, activeOrdersKey, openOrdersKey}
	args := []interface{}{
		coinName + "_frozen",
		coinName,
		unfreezeAmount,
		res.OrderId,
	}

	logx.Infow("execute redis cancel lua",
		logx.Field("orderId", res.OrderId),
		logx.Field("uid", res.Uid),
		logx.Field("coin", coinName),
		logx.Field("unfreezeAmount", unfreezeAmount))
	_, err = atomicCancelScript.Run(ctx, l.svcCtx.RedisCli, keys, args...).Result()
	if err != nil {
		logx.Errorw("execute cancel lua failed",
			logx.Field("orderId", res.OrderId),
			logx.Field("error", err.Error()))
		return fmt.Errorf("execute atomicCancelScript failed: %w", err)
	}
	logx.Infow("redis cancel lua success", logx.Field("orderId", res.OrderId))

	if err := persistOrderFinalTx(ctx, l.svcCtx, cancelDoc); err != nil {
		logx.Errorw("persist cancel order final failed",
			logx.Field("orderId", res.OrderId),
			logx.Field("error", err.Error()))
		return fmt.Errorf("persist canceled order to mongodb failed: %w", err)
	}

	if err := storeConsumedMessageId(); err != nil {
		logx.Errorw("ack cancel message failed",
			logx.Field("messageId", messageId),
			logx.Field("orderId", res.OrderId),
			logx.Field("error", err.Error()))
		return err
	}
	logx.Infow("handle cancel result success",
		logx.Field("messageId", messageId),
		logx.Field("orderId", res.OrderId),
		logx.Field("uid", res.Uid))
	return nil
}

// HandleAcceptOrder 订单入簿确认：有序索引已在下单时写入。
func (l *HandleMatchResultLogic) HandleAcceptOrder(acceptResult *matchMq.AcceptedResult, messageId int64) error {
	logx.Infow("handle accept result success",
		logx.Field("messageId", messageId),
		logx.Field("orderId", acceptResult.OrderId),
		logx.Field("uid", acceptResult.Uid),
		logx.Field("symbol", acceptResult.SymbolName))
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
	return rediskeys.UserSlotTag(uid)
}

// frozenDeductInt 以本笔成交冻结扣减量为准；撮合回传仅作兜底。
func (l *HandleMatchResultLogic) frozenDeductInt(currency, fromMatch, fallback string) (int64, error) {
	amount := fallback
	if amount == "" {
		amount = fromMatch
	}
	if amount == "" {
		return 0, fmt.Errorf("unfrozen amount empty for currency %s", currency)
	}
	return utils.ToDBInteger(currency, amount)
}
