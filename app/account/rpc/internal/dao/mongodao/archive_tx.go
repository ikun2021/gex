package mongodao

import (
	"context"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SettleArchiveStore 撮合归档写入（事务 + 批量 Insert）。
type SettleArchiveStore struct {
	client     *mongo.Client
	matchTrade *MatchTradeRepo
	orderFinal *OrderFinalRepo
}

func NewSettleArchiveStore(client *mongo.Client, matchTrade *MatchTradeRepo, orderFinal *OrderFinalRepo) *SettleArchiveStore {
	return &SettleArchiveStore{
		client:     client,
		matchTrade: matchTrade,
		orderFinal: orderFinal,
	}
}

// InsertSettleData 在同一事务中批量插入撮合明细与订单终态（仅 InsertMany，不 Upsert）。
func (s *SettleArchiveStore) InsertSettleData(ctx context.Context, trades []*MatchTrade, orders []*OrderFinal) error {
	if s == nil || s.client == nil {
		return nil
	}
	if len(trades) == 0 && len(orders) == 0 {
		return nil
	}

	normalizeMatchTrades(trades)
	normalizeOrderFinals(orders)

	session, err := s.client.StartSession()
	if err != nil {
		return fmt.Errorf("start mongo session failed: %w", err)
	}
	defer session.EndSession(ctx)

	var insertedTrades, skippedTrades, insertedOrders, skippedOrders int
	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
		if s.matchTrade != nil && len(trades) > 0 {
			toInsert, err := s.matchTrade.filterNotExists(sc, trades)
			if err != nil {
				return nil, err
			}
			skippedTrades = len(trades) - len(toInsert)
			if err := s.matchTrade.batchInsert(sc, toInsert); err != nil {
				return nil, err
			}
			insertedTrades = len(toInsert)
		}
		if s.orderFinal != nil && len(orders) > 0 {
			toInsert, err := s.orderFinal.filterNotExists(sc, orders)
			if err != nil {
				return nil, err
			}
			skippedOrders = len(orders) - len(toInsert)
			if err := s.orderFinal.batchInsert(sc, toInsert); err != nil {
				return nil, err
			}
			insertedOrders = len(toInsert)
		}
		return nil, nil
	})
	if err != nil {
		logx.Errorw("mongo transaction insert settle data failed",
			logx.Field("tradeTotal", len(trades)),
			logx.Field("orderTotal", len(orders)),
			logx.Field("error", err.Error()))
		return fmt.Errorf("mongo transaction insert settle data failed: %w", err)
	}
	logx.Infow("mongo transaction insert settle data success",
		logx.Field("matchTradeInserted", insertedTrades),
		logx.Field("matchTradeSkipped", skippedTrades),
		logx.Field("orderFinalInserted", insertedOrders),
		logx.Field("orderFinalSkipped", skippedOrders))
	return nil
}

func normalizeMatchTrades(trades []*MatchTrade) {
	now := time.Now().UnixMilli()
	for _, t := range trades {
		if t == nil {
			continue
		}
		if t.ID.IsZero() {
			t.ID = primitive.NewObjectID()
		}
		if t.CreatedAt == 0 {
			t.CreatedAt = now
		}
	}
}

func normalizeOrderFinals(orders []*OrderFinal) {
	now := time.Now().UnixMilli()
	for _, o := range orders {
		if o == nil {
			continue
		}
		if o.ID.IsZero() {
			o.ID = primitive.NewObjectID()
		}
		if o.ArchivedAt == 0 {
			o.ArchivedAt = now
		}
		if o.FinishedAt == 0 {
			o.FinishedAt = now
		}
		if o.UpdatedAt == 0 {
			o.UpdatedAt = now
		}
	}
}

func (r *MatchTradeRepo) filterNotExists(ctx context.Context, docs []*MatchTrade) ([]*MatchTrade, error) {
	ids := make([]string, 0, len(docs))
	for _, d := range docs {
		if d != nil && d.MatchSubID != "" {
			ids = append(ids, d.MatchSubID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}

	existing, err := r.findExistingMatchSubIDs(ctx, ids)
	if err != nil {
		return nil, err
	}

	out := make([]*MatchTrade, 0, len(docs))
	for _, d := range docs {
		if d == nil || d.MatchSubID == "" {
			continue
		}
		if _, ok := existing[d.MatchSubID]; ok {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

func (r *MatchTradeRepo) findExistingMatchSubIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	cur, err := r.coll.Find(ctx, bson.M{"match_sub_id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{"match_sub_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	existing := make(map[string]struct{}, len(ids))
	for cur.Next(ctx) {
		var row struct {
			MatchSubID string `bson:"match_sub_id"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, err
		}
		existing[row.MatchSubID] = struct{}{}
	}
	return existing, cur.Err()
}

func (r *MatchTradeRepo) batchInsert(ctx context.Context, docs []*MatchTrade) error {
	if len(docs) == 0 {
		return nil
	}
	models := make([]interface{}, len(docs))
	for i, d := range docs {
		models[i] = d
	}
	_, err := r.coll.InsertMany(ctx, models)
	return err
}

func (r *OrderFinalRepo) filterNotExists(ctx context.Context, docs []*OrderFinal) ([]*OrderFinal, error) {
	ids := make([]string, 0, len(docs))
	for _, d := range docs {
		if d != nil && d.OrderID != "" {
			ids = append(ids, d.OrderID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}

	existing, err := r.findExistingOrderIDs(ctx, ids)
	if err != nil {
		return nil, err
	}

	out := make([]*OrderFinal, 0, len(docs))
	for _, d := range docs {
		if d == nil || d.OrderID == "" {
			continue
		}
		if _, ok := existing[d.OrderID]; ok {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

func (r *OrderFinalRepo) findExistingOrderIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	cur, err := r.coll.Find(ctx, bson.M{"order_id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{"order_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	existing := make(map[string]struct{}, len(ids))
	for cur.Next(ctx) {
		var row struct {
			OrderID string `bson:"order_id"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, err
		}
		existing[row.OrderID] = struct{}{}
	}
	return existing, cur.Err()
}

func (r *OrderFinalRepo) batchInsert(ctx context.Context, docs []*OrderFinal) error {
	if len(docs) == 0 {
		return nil
	}
	models := make([]interface{}, len(docs))
	for i, d := range docs {
		models[i] = d
	}
	_, err := r.coll.InsertMany(ctx, models)
	return err
}
