package dao

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TickDoc 成交明细（逐笔）。
type TickDoc struct {
	PkID        int64  `bson:"pk_id"`
	MatchID     string `bson:"match_id"`
	MatchSubID  string `bson:"match_sub_id"`
	OrderID     string `bson:"order_id"`
	UserID      int64  `bson:"user_id"`
	Symbol      string `bson:"symbol"`
	Price       string `bson:"price"`
	BaseAmount  string `bson:"base_amount"`
	QuoteAmount string `bson:"quote_amount"`
	Side        int32  `bson:"side"`
	Role        int32  `bson:"role"`
	CreatedAt   int64  `bson:"created_at"`
}

type TickRepo struct {
	coll *mongo.Collection
}

func NewTickRepo(coll *mongo.Collection) *TickRepo {
	return &TickRepo{coll: coll}
}

func (r *TickRepo) EnsureIndex(ctx context.Context) error {
	_, err := r.coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "pk_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "symbol", Value: 1}, {Key: "created_at", Value: -1}},
		},
	})
	return err
}

func (r *TickRepo) ListBySymbol(ctx context.Context, symbol string, limit int64) ([]*TickDoc, error) {
	if limit <= 0 {
		limit = 100
	}
	filter := bson.M{"symbol": symbol}
	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetLimit(limit)
	cur, err := r.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []*TickDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (r *TickRepo) CountBySymbol(ctx context.Context, symbol string) (int64, error) {
	return r.coll.CountDocuments(ctx, bson.M{"symbol": symbol})
}

func (r *TickRepo) InsertMany(ctx context.Context, docs []*TickDoc) error {
	if len(docs) == 0 {
		return nil
	}
	toInsert := make([]interface{}, 0, len(docs))
	for _, d := range docs {
		if d != nil {
			toInsert = append(toInsert, d)
		}
	}
	if len(toInsert) == 0 {
		return nil
	}
	_, err := r.coll.InsertMany(ctx, toInsert, options.InsertMany().SetOrdered(false))
	if err == nil || isDuplicateKeyBulkErr(err) {
		return nil
	}
	return err
}

// isDuplicateKeyBulkErr 批量写入若全部为 pk_id 重复（重试/部分成功后的幂等写入），视为成功。
func isDuplicateKeyBulkErr(err error) bool {
	var bulkErr mongo.BulkWriteException
	if errors.As(err, &bulkErr) {
		if len(bulkErr.WriteErrors) == 0 {
			return false
		}
		for _, we := range bulkErr.WriteErrors {
			if we.Code != 11000 {
				return false
			}
		}
		return true
	}
	return mongo.IsDuplicateKeyError(err)
}
