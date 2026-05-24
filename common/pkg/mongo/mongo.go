package mongo

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Conf struct {
	URI                    string `json:"uri"`
	Database               string `json:"database"`
	OrderFinalCollection   string `json:"orderFinalCollection,optional"`
	MatchTradeCollection   string `json:"matchTradeCollection,optional"`
	UserCollection         string `json:"userCollection,optional"`
	KlineCollection        string `json:"klineCollection,optional"`
	TickCollection         string `json:"tickCollection,optional"`
	ConnectTimeoutSeconds  int    `json:"connectTimeoutSeconds,optional"`
}

func (c *Conf) MustNewClient() *mongo.Client {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.connectTimeout())*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(c.URI))
	if err != nil {
		logx.Severef("connect mongodb failed: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		logx.Severef("ping mongodb failed: %v", err)
	}
	return client
}

func (c *Conf) connectTimeout() int {
	if c.ConnectTimeoutSeconds <= 0 {
		return 10
	}
	return c.ConnectTimeoutSeconds
}

func (c *Conf) OrderFinalColl(client *mongo.Client) *mongo.Collection {
	return client.Database(c.Database).Collection(c.orderFinalCollection())
}

func (c *Conf) orderFinalCollection() string {
	if c.OrderFinalCollection == "" {
		return "order_final"
	}
	return c.OrderFinalCollection
}

func (c *Conf) MatchTradeColl(client *mongo.Client) *mongo.Collection {
	return client.Database(c.Database).Collection(c.matchTradeCollection())
}

func (c *Conf) UserColl(client *mongo.Client) *mongo.Collection {
	return client.Database(c.Database).Collection(c.userCollection())
}

func (c *Conf) userCollection() string {
	if c.UserCollection == "" {
		return "user"
	}
	return c.UserCollection
}

func (c *Conf) matchTradeCollection() string {
	if c.MatchTradeCollection == "" {
		return "match_trade"
	}
	return c.MatchTradeCollection
}

func (c *Conf) KlineColl(client *mongo.Client) *mongo.Collection {
	return client.Database(c.Database).Collection(c.klineCollection())
}

func (c *Conf) klineCollection() string {
	if c.KlineCollection == "" {
		return "kline_history"
	}
	return c.KlineCollection
}

func (c *Conf) TickColl(client *mongo.Client) *mongo.Collection {
	return client.Database(c.Database).Collection(c.tickCollection())
}

func (c *Conf) tickCollection() string {
	if c.TickCollection == "" {
		return "tick"
	}
	return c.TickCollection
}
