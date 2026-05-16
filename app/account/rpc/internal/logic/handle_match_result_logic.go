package logic

import (
	"context"
	"fmt"
	"strings"
	"time"


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
// ARGV[i*10 + 6]: idempotentKey
// ARGV[i*10 + 7]: takerFrozenField, takerAvailField, takerFrozenDeduct, takerAvailAdd
// ARGV[i*10 + 8]: makerFrozenField, makerAvailField, makerFrozenDeduct, makerAvailAdd
// ARGV[i*10 + 9]: takerOrderId, takerOrderStatus
// ARGV[index + 10]: makerOrderId, makerOrderStatus
// 每条记录 stride=20 个 ARGV 槽位
var atomicSettleBatchScript = redis.NewScript(`
local count = tonumber(ARGV[1])
local stride = 20
for i=0, count-1 do
    local offset = i * stride + 1
    local takerBalanceKey = ARGV[offset + 1]
    local makerBalanceKey = ARGV[offset + 2]
    local takerOpenOrdersKey = ARGV[offset + 3]
    local makerOpenOrdersKey = ARGV[offset + 4]
    local idempotentKey = ARGV[offset + 5]
    
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
        local takerActiveOrdersKey = ARGV[offset + 19]
        local makerActiveOrdersKey = ARGV[offset + 20]

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

        redis.call("SET", idempotentKey, "1", "EX", idempotentEx)
    end
end
return 1
`)

// Lua脚本：原子性处理订单取消（解冻资产 + 删除活跃快照 + 移除有序索引）
// KEYS[1]: balanceKey
// KEYS[2]: activeOrdersKey
// KEYS[3]: openOrdersKey
// KEYS[4]: idempotentKey
var atomicCancelScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[4]) == 1 then
    return 1
end
redis.call("HINCRBY", KEYS[1], ARGV[1], -tonumber(ARGV[3]))
redis.call("HINCRBY", KEYS[1], ARGV[2], tonumber(ARGV[3]))
redis.call("HDEL", KEYS[2], ARGV[4])
redis.call("ZREM", KEYS[3], ARGV[4])
redis.call("SET", KEYS[4], "1", "EX", ARGV[5])
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
	takerBalanceKey := rediskeys.UserBalanceKey(tagTaker, taker.Uid)
	makerBalanceKey := rediskeys.UserBalanceKey(tagMaker, maker.Uid)
	takerOpenOrdersKey := rediskeys.UserOpenOrdersKey(tagTaker, taker.Uid)
	makerOpenOrdersKey := rediskeys.UserOpenOrdersKey(tagMaker, maker.Uid)
	takerActiveOrdersKey := rediskeys.UserActiveOrdersKey(tagTaker, taker.Uid)
	makerActiveOrdersKey := rediskeys.UserActiveOrdersKey(tagMaker, maker.Uid)
	idempotentKey := fmt.Sprintf("match_processed:%s:%s", record.MatchSubId, tagTaker)

	return []interface{}{
		takerBalanceKey,
		makerBalanceKey,
		takerOpenOrdersKey,
		makerOpenOrdersKey,
		idempotentKey,
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
		3600 * 24,
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
	balanceKey := rediskeys.UserBalanceKey(tag, res.Uid)
	activeOrdersKey := rediskeys.UserActiveOrdersKey(tag, res.Uid)
	openOrdersKey := rediskeys.UserOpenOrdersKey(tag, res.Uid)
	idempotentKey := fmt.Sprintf("match_processed:cancel:%d:%s", messageId, tag)

	keys := []string{balanceKey, activeOrdersKey, openOrdersKey, idempotentKey}
	args := []interface{}{
		coinName + "_frozen",
		coinName,
		unfreezeAmount,
		res.OrderId,
		3600 * 24,
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

// HandleAcceptOrder 订单入簿确认：有序索引已在下单时写入，此处仅做幂等标记。
func (l *HandleMatchResultLogic) HandleAcceptOrder(acceptResult *matchMq.AcceptedResult, messageId int64) error {
	tag := l.getTag(acceptResult.Uid)
	idempotentKey := fmt.Sprintf("match_processed:accept:%d:%s", messageId, tag)
	ctx := context.Background()

	ok, err := l.svcCtx.RedisCli.SetNX(ctx, idempotentKey, "1", 24*time.Hour).Result()
	if err != nil {
		return fmt.Errorf("mark accept processed failed: %w", err)
	}
	if !ok {
		return nil
	}
	_ = acceptResult
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
