package mongodao

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MatchTradeSide 单笔撮合中某一侧订单的成交快照。
type MatchTradeSide struct {
	PkID                int64  `bson:"pk_id"`
	UserID              int64  `bson:"user_id"`
	OrderID             string `bson:"order_id"`
	FilledBaseAmount    string `bson:"filled_base_amount"`
	UnFilledBaseAmount  string `bson:"un_filled_base_amount"`
	FilledQuoteAmount   string `bson:"filled_quote_amount"`
	UnFilledQuoteAmount string `bson:"un_filled_quote_amount"`
	OrderStatus         int32  `bson:"order_status"`
}

// MatchTrade 撮合成交明细（按 match_sub_id 一条记录对应一次撮合匹配）。
type MatchTrade struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	MatchSubID  string             `bson:"match_sub_id"`
	MatchID     string             `bson:"match_id"`
	SymbolID    int32              `bson:"symbol_id"`
	SymbolName  string             `bson:"symbol_name"`
	BaseCoinID  int32              `bson:"base_coin_id"`
	QuoteCoinID int32              `bson:"quote_coin_id"`
	TakerIsBuy  bool               `bson:"taker_is_buy"`
	Price       string             `bson:"price"`
	BaseAmount  string             `bson:"base_amount"`
	QuoteAmount string             `bson:"quote_amount"`
	BeginPrice  string             `bson:"begin_price,omitempty"`
	EndPrice    string             `bson:"end_price,omitempty"`
	HighPrice   string             `bson:"high_price,omitempty"`
	LowPrice    string             `bson:"low_price,omitempty"`
	MatchTime   int64              `bson:"match_time"`
	Taker       MatchTradeSide     `bson:"taker"`
	Maker       MatchTradeSide     `bson:"maker"`
	CreatedAt   int64              `bson:"created_at"`
}

type MatchTradeRepo struct {
	coll *mongo.Collection
}

func NewMatchTradeRepo(coll *mongo.Collection) *MatchTradeRepo {
	return &MatchTradeRepo{coll: coll}
}

func (r *MatchTradeRepo) EnsureIndex(ctx context.Context) error {
	_, err := r.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "match_sub_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}
