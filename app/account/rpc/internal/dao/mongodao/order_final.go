package mongodao

import (
	"context"

	"github.com/ikun2021/gex/common/proto/enum"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OrderFinal 订单终态（全部成交 / 已撤销 / 废单等），不含撮合明细。
type OrderFinal struct {
	ID                  primitive.ObjectID `bson:"_id,omitempty"`
	OrderID             string             `bson:"order_id"`
	UserID              int64              `bson:"user_id"`
	PkID                int64              `bson:"pk_id"`
	SymbolID            int32              `bson:"symbol_id"`
	SymbolName          string             `bson:"symbol_name"`
	Price               string             `bson:"price"`
	BaseAmount          string             `bson:"base_amount"`
	QuoteAmount         string             `bson:"quote_amount"`
	Side                int32              `bson:"side"`
	Status              int32              `bson:"status"`
	OrderType           int32              `bson:"order_type"`
	FilledBaseAmount    string             `bson:"filled_base_amount"`
	UnFilledBaseAmount  string             `bson:"un_filled_base_amount"`
	FilledQuoteAmount   string             `bson:"filled_quote_amount"`
	UnFilledQuoteAmount string             `bson:"un_filled_quote_amount"`
	FilledAvgPrice      string             `bson:"filled_avg_price"`
	FinishReason        string             `bson:"finish_reason"`
	CreatedAt           int64              `bson:"created_at"`
	UpdatedAt           int64              `bson:"updated_at"`
	FinishedAt          int64              `bson:"finished_at"`
	ArchivedAt          int64              `bson:"archived_at"`
}

type OrderFinalRepo struct {
	coll *mongo.Collection
}

func NewOrderFinalRepo(coll *mongo.Collection) *OrderFinalRepo {
	return &OrderFinalRepo{coll: coll}
}

func (r *OrderFinalRepo) EnsureIndex(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "order_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "pk_id", Value: -1}},
		},
	}
	_, err := r.coll.Indexes().CreateMany(ctx, models)
	return err
}

// OrderFinalListQuery 历史委托分页查询（pk_id 游标倒序）。
type OrderFinalListQuery struct {
	UserID     int64
	StatusList []int32
	SymbolName string
	CursorID   int64
	PageSize   int64
}

func (r *OrderFinalRepo) buildListFilter(q OrderFinalListQuery) bson.M {
	filter := bson.M{"user_id": q.UserID}
	if q.CursorID > 0 {
		filter["pk_id"] = bson.M{"$lt": q.CursorID}
	}
	if len(q.StatusList) > 0 {
		filter["status"] = bson.M{"$in": q.StatusList}
	}
	if q.SymbolName != "" {
		filter["symbol_name"] = q.SymbolName
	}
	return filter
}

func (r *OrderFinalRepo) CountByUser(ctx context.Context, q OrderFinalListQuery) (int64, error) {
	return r.coll.CountDocuments(ctx, r.buildListFilter(q))
}

func (r *OrderFinalRepo) ListByUser(ctx context.Context, q OrderFinalListQuery) ([]*OrderFinal, error) {
	pageSize := q.PageSize
	if pageSize <= 0 {
		pageSize = 20
	}
	cur, err := r.coll.Find(ctx, r.buildListFilter(q), options.Find().
		SetSort(bson.D{{Key: "pk_id", Value: -1}}).
		SetLimit(pageSize))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []*OrderFinal
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func IsTerminalStatus(status enum.OrderStatus) bool {
	switch status {
	case enum.OrderStatus_ALLFilled, enum.OrderStatus_Canceled, enum.OrderStatus_Wasted:
		return true
	default:
		return false
	}
}
