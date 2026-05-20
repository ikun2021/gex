package dao

import (
	"context"
	"time"

	"github.com/ikun2021/gex/common/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// KlineHistory 历史 K 线文档。
type KlineHistory struct {
	Symbol    string `bson:"symbol"`
	SymbolID  int32  `bson:"symbol_id"`
	KlineType int32  `bson:"kline_type"`
	StartTime int64  `bson:"start_time"`
	EndTime   int64  `bson:"end_time"`
	Open      string `bson:"open"`
	High      string `bson:"high"`
	Low       string `bson:"low"`
	Close     string `bson:"close"`
	Volume    string `bson:"volume"`
	Amount    string `bson:"amount"`
	Range     string `bson:"range"`
	UpdatedAt int64  `bson:"updated_at"`
}

type KlineHistoryRepo struct {
	coll *mongo.Collection
}

func NewKlineHistoryRepo(coll *mongo.Collection) *KlineHistoryRepo {
	return &KlineHistoryRepo{coll: coll}
}

func (r *KlineHistoryRepo) EnsureIndex(ctx context.Context) error {
	_, err := r.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "symbol", Value: 1},
			{Key: "kline_type", Value: 1},
			{Key: "start_time", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	return err
}

func MemoryKlineToHistory(k *MemoryKline, symbol models.Symbol) *KlineHistory {
	if k == nil {
		return nil
	}
	return &KlineHistory{
		Symbol:    symbol.Name,
		SymbolID:  symbol.Id,
		KlineType: int32(k.KlineType),
		StartTime: k.StartTime,
		EndTime:   k.EndTime,
		Open:      k.Open.String(),
		High:      k.High.String(),
		Low:       k.Low.String(),
		Close:     k.Close.String(),
		Volume:    k.Volume.String(),
		Amount:    k.Amount.String(),
		Range:     k.Range,
		UpdatedAt: time.Now().UnixMilli(),
	}
}

func (r *KlineHistoryRepo) UpsertMany(ctx context.Context, docs []*KlineHistory) error {
	if len(docs) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	models := make([]mongo.WriteModel, 0, len(docs))
	for _, doc := range docs {
		if doc == nil {
			continue
		}
		if doc.UpdatedAt == 0 {
			doc.UpdatedAt = now
		}
		filter := bson.M{
			"symbol":     doc.Symbol,
			"kline_type": doc.KlineType,
			"start_time": doc.StartTime,
		}
		update := bson.M{"$set": doc}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}
	_, err := r.coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

func (r *KlineHistoryRepo) ListByRange(ctx context.Context, symbol string, klineType int32, startTime, endTime int64) ([]*KlineHistory, error) {
	filter := bson.M{
		"symbol":     symbol,
		"kline_type": klineType,
	}
	if startTime > 0 || endTime > 0 {
		timeFilter := bson.M{}
		if startTime > 0 {
			timeFilter["$gte"] = startTime
		}
		if endTime > 0 {
			timeFilter["$lte"] = endTime
		}
		filter["start_time"] = timeFilter
	}
	opts := options.Find().SetSort(bson.D{{Key: "start_time", Value: 1}})
	cur, err := r.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []*KlineHistory
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (r *KlineHistoryRepo) ListSince(ctx context.Context, symbol string, klineType int32, startTime int64) ([]*KlineHistory, error) {
	filter := bson.M{
		"symbol":     symbol,
		"kline_type": klineType,
		"start_time": bson.M{"$gte": startTime},
	}
	opts := options.Find().SetSort(bson.D{{Key: "start_time", Value: 1}})
	cur, err := r.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []*KlineHistory
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func HistoryToMemoryKline(d *KlineHistory) *MemoryKline {
	if d == nil {
		return nil
	}
	return &MemoryKline{
		KlineType: KlineType(d.KlineType),
		StartTime: d.StartTime,
		EndTime:   d.EndTime,
		Open:      mustDecimal(d.Open),
		High:      mustDecimal(d.High),
		Low:       mustDecimal(d.Low),
		Close:     mustDecimal(d.Close),
		Volume:    mustDecimal(d.Volume),
		Amount:    mustDecimal(d.Amount),
		Range:     d.Range,
	}
}
