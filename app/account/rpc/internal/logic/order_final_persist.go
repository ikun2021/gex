package logic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ikun2021/gex/app/account/rpc/internal/dao/mongodao"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/proto/enum"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/rediskeys"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/proto"
)

const (
	finishReasonMatch  = "match"
	finishReasonCancel = "cancel"
)

func (l *HandleMatchResultLogic) loadActiveOrderInfo(ctx context.Context, uid int64, orderID string) (*pb.OrderInfo, error) {
	tag := l.getTag(uid)
	key := rediskeys.UserActiveOrdersKey(tag, uid)
	data, err := l.svcCtx.RedisCli.HGet(ctx, key, orderID).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	var info pb.OrderInfo
	if err := proto.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("unmarshal order info: %w", err)
	}
	return &info, nil
}

func collectMatchTradesFromMatch(match *matchMq.MatchResult) []*mongodao.MatchTrade {
	trades := make([]*mongodao.MatchTrade, 0, len(match.MatchedRecord))
	for _, record := range match.MatchedRecord {
		trades = append(trades, buildMatchTrade(match, record))
	}
	return trades
}

func buildMatchTrade(match *matchMq.MatchResult, record *matchMq.MatchResult_MatchedRecord) *mongodao.MatchTrade {
	return &mongodao.MatchTrade{
		MatchSubID:  record.MatchSubId,
		MatchID:     match.MatchId,
		SymbolID:    match.SymbolId,
		SymbolName:  match.SymbolName,
		BaseCoinID:  match.BaseCoinId,
		QuoteCoinID: match.QuoteCoinId,
		TakerIsBuy:  match.TakerIsBuy,
		Price:       record.Price,
		BaseAmount:  record.BaseAmount,
		QuoteAmount: record.QuoteAmount,
		BeginPrice:  match.BeginPrice,
		EndPrice:    match.EndPrice,
		HighPrice:   match.HighPrice,
		LowPrice:    match.LowPrice,
		MatchTime:   match.MatchTime,
		Taker:       buildMatchTradeSide(record.Taker),
		Maker:       buildMatchTradeSide(record.Maker),
		CreatedAt:   time.Now().UnixMilli(),
	}
}

func buildMatchTradeSide(resp *matchMq.OrderResp) mongodao.MatchTradeSide {
	if resp == nil {
		return mongodao.MatchTradeSide{}
	}
	return mongodao.MatchTradeSide{
		PkID:                resp.Id,
		UserID:              resp.Uid,
		OrderID:             resp.OrderId,
		FilledBaseAmount:    resp.FilledBaseAmount,
		UnFilledBaseAmount:  resp.UnFilledBaseAmount,
		FilledQuoteAmount:   resp.FilledQuoteAmount,
		UnFilledQuoteAmount: resp.UnFilledQuoteAmount,
		OrderStatus:         int32(resp.OrderStatus),
	}
}

func (l *HandleMatchResultLogic) collectTerminalOrdersFromMatch(
	ctx context.Context,
	match *matchMq.MatchResult,
) (map[string]*mongodao.OrderFinal, error) {
	out := make(map[string]*mongodao.OrderFinal)
	for _, record := range match.MatchedRecord {
		if mongodao.IsTerminalStatus(record.Taker.OrderStatus) {
			info, err := l.loadActiveOrderInfo(ctx, record.Taker.Uid, record.Taker.OrderId)
			if err != nil {
				return nil, err
			}
			out[record.Taker.OrderId] = buildOrderFinal(record.Taker, info, match.SymbolId, match.SymbolName, finishReasonMatch, match.MatchTime)
			logx.Infow("collect terminal order from match",
				logx.Field("matchId", match.MatchId),
				logx.Field("matchSubId", record.MatchSubId),
				logx.Field("role", "taker"),
				logx.Field("orderId", record.Taker.OrderId),
				logx.Field("uid", record.Taker.Uid),
				logx.Field("status", int32(record.Taker.OrderStatus)),
				logx.Field("hasSnapshot", info != nil))
		}
		if mongodao.IsTerminalStatus(record.Maker.OrderStatus) {
			info, err := l.loadActiveOrderInfo(ctx, record.Maker.Uid, record.Maker.OrderId)
			if err != nil {
				return nil, err
			}
			out[record.Maker.OrderId] = buildOrderFinal(record.Maker, info, match.SymbolId, match.SymbolName, finishReasonMatch, match.MatchTime)
			logx.Infow("collect terminal order from match",
				logx.Field("matchId", match.MatchId),
				logx.Field("matchSubId", record.MatchSubId),
				logx.Field("role", "maker"),
				logx.Field("orderId", record.Maker.OrderId),
				logx.Field("uid", record.Maker.Uid),
				logx.Field("status", int32(record.Maker.OrderStatus)),
				logx.Field("hasSnapshot", info != nil))
		}
	}
	return out, nil
}

func buildOrderFinal(
	resp *matchMq.OrderResp,
	info *pb.OrderInfo,
	symbolID int32,
	symbolName string,
	finishReason string,
	finishedAt int64,
) *mongodao.OrderFinal {
	now := time.Now().UnixMilli()
	doc := &mongodao.OrderFinal{
		OrderID:             resp.OrderId,
		UserID:              resp.Uid,
		PkID:                resp.Id,
		SymbolID:            symbolID,
		SymbolName:          symbolName,
		Status:              int32(resp.OrderStatus),
		FilledBaseAmount:    resp.FilledBaseAmount,
		UnFilledBaseAmount:  resp.UnFilledBaseAmount,
		FilledQuoteAmount:   resp.FilledQuoteAmount,
		UnFilledQuoteAmount: resp.UnFilledQuoteAmount,
		FinishReason:        finishReason,
		FinishedAt:          finishedAt,
		ArchivedAt:          now,
		UpdatedAt:           now,
	}
	if doc.FinishedAt == 0 {
		doc.FinishedAt = now
	}
	if info != nil {
		doc.Price = info.Price
		doc.BaseAmount = info.BaseAmount
		doc.QuoteAmount = info.QuoteAmount
		doc.Side = int32(info.Side)
		doc.OrderType = int32(info.OrderType)
		doc.FilledAvgPrice = info.FilledAvgPrice
		doc.CreatedAt = info.CreatedAt
		if info.SymbolId != 0 {
			doc.SymbolID = info.SymbolId
		}
		if info.SymbolName != "" {
			doc.SymbolName = info.SymbolName
		}
		if info.Id != 0 {
			doc.PkID = info.Id
		}
	}
	applyFilledToOrderFinal(doc, resp)
	return doc
}

func buildOrderFinalFromCancel(info *pb.OrderInfo, cancel *matchMq.CancelResult, finishReason string) *mongodao.OrderFinal {
	now := time.Now().UnixMilli()
	doc := &mongodao.OrderFinal{
		OrderID:      cancel.OrderId,
		UserID:       cancel.Uid,
		PkID:         cancel.Id,
		Status:       int32(enum.OrderStatus_Canceled),
		FinishReason: finishReason,
		FinishedAt:   now,
		ArchivedAt:   now,
		UpdatedAt:    now,
	}
	if info != nil {
		doc.SymbolID = info.SymbolId
		doc.SymbolName = info.SymbolName
		doc.Price = info.Price
		doc.BaseAmount = info.BaseAmount
		doc.QuoteAmount = info.QuoteAmount
		doc.Side = int32(info.Side)
		doc.OrderType = int32(info.OrderType)
		doc.FilledBaseAmount = info.FilledBaseAmount
		doc.UnFilledBaseAmount = info.UnFilledBaseAmount
		doc.FilledQuoteAmount = info.FilledQuoteAmount
		doc.UnFilledQuoteAmount = info.UnFilledQuoteAmount
		doc.FilledAvgPrice = info.FilledAvgPrice
		doc.CreatedAt = info.CreatedAt
		if info.Id != 0 {
			doc.PkID = info.Id
		}
	}
	return doc
}

func applyFilledToOrderFinal(doc *mongodao.OrderFinal, resp *matchMq.OrderResp) {
	if resp.FilledBaseAmount != "" {
		doc.FilledBaseAmount = resp.FilledBaseAmount
	}
	if resp.UnFilledBaseAmount != "" {
		doc.UnFilledBaseAmount = resp.UnFilledBaseAmount
	}
	if resp.FilledQuoteAmount != "" {
		doc.FilledQuoteAmount = resp.FilledQuoteAmount
	}
	if resp.UnFilledQuoteAmount != "" {
		doc.UnFilledQuoteAmount = resp.UnFilledQuoteAmount
	}
}

func orderFinalMapToSlice(docs map[string]*mongodao.OrderFinal) []*mongodao.OrderFinal {
	if len(docs) == 0 {
		return nil
	}
	out := make([]*mongodao.OrderFinal, 0, len(docs))
	for _, doc := range docs {
		out = append(out, doc)
	}
	return out
}

func persistSettleArchiveTx(ctx context.Context, sc *svc.ServiceContext, trades []*mongodao.MatchTrade, orderFinals map[string]*mongodao.OrderFinal) error {
	if sc.SettleArchive == nil {
		logx.Infow("skip mongo settle archive, store not configured")
		return nil
	}
	matchID := ""
	if len(trades) > 0 && trades[0] != nil {
		matchID = trades[0].MatchID
	}
	orderList := orderFinalMapToSlice(orderFinals)
	logx.Infow("persist settle archive to mongodb start",
		logx.Field("matchId", matchID),
		logx.Field("matchTradeCount", len(trades)),
		logx.Field("orderFinalCount", len(orderList)))
	if err := sc.SettleArchive.InsertSettleData(ctx, trades, orderList); err != nil {
		return err
	}
	logx.Infow("persist settle archive to mongodb success",
		logx.Field("matchId", matchID),
		logx.Field("matchTradeCount", len(trades)),
		logx.Field("orderFinalCount", len(orderList)))
	return nil
}

func persistOrderFinalTx(ctx context.Context, sc *svc.ServiceContext, doc *mongodao.OrderFinal) error {
	if sc.SettleArchive == nil || doc == nil {
		logx.Infow("skip mongo order final persist",
			logx.Field("hasStore", sc.SettleArchive != nil),
			logx.Field("hasDoc", doc != nil))
		return nil
	}
	logx.Infow("persist order final to mongodb start",
		logx.Field("orderId", doc.OrderID),
		logx.Field("uid", doc.UserID),
		logx.Field("status", doc.Status),
		logx.Field("finishReason", doc.FinishReason))
	if err := sc.SettleArchive.InsertSettleData(ctx, nil, []*mongodao.OrderFinal{doc}); err != nil {
		return err
	}
	logx.Infow("persist order final to mongodb success",
		logx.Field("orderId", doc.OrderID),
		logx.Field("uid", doc.UserID))
	return nil
}
