package svc

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/config"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao"
	"github.com/ikun2021/gex/common/defines"
	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/ikun2021/zlog"
	gpushPb "github.com/luxun9527/gpush/proto"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
	"go.mongodb.org/mongo-driver/mongo"
)

// MatchSymbolConsumers 单个交易对的撮合结果消费者（tick / ticker / kline）。
type MatchSymbolConsumers struct {
	Tick   pulsar.Consumer
	Ticker pulsar.Consumer
	Kline  pulsar.Consumer
}

type ServiceContext struct {
	Config           *config.Config
	MongoCli         *mongo.Client
	KlineHistoryRepo *dao.KlineHistoryRepo
	TickRepo         *dao.TickRepo
	RedisClient      *redis.Redis
	PulsarClient     pulsar.Client
	MatchConsumers   map[string]*MatchSymbolConsumers
	TradeIdgen       *idgen.DefaultIdGenerator
	WsClient         gpushPb.ProxyClient
}

// BuildMatchOutputTopic 构建撮合输出 Topic（与 account rpc 一致）。
func BuildMatchOutputTopic(symbolName string) string {
	return pulsarConfig.Topic{
		Tenant:    pulsarConfig.PublicTenant,
		Namespace: pulsarConfig.GexNamespace,
		Topic:     defines.MatchTopicOutputPrefix + symbolName,
	}.BuildTopic()
}

func NewServiceContext(c *config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()

	idgen.SetIdGenerator(idgen.NewIdGeneratorOptions(1))
	tradeIdgen := idgen.NewDefaultIdGenerator(idgen.NewIdGeneratorOptions(1))

	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed %v", err)
	}

	matchConsumers := make(map[string]*MatchSymbolConsumers, len(c.Symbol))
	for _, v := range c.Symbol {
		topic := BuildMatchOutputTopic(v.Name)
		logx.Infow("subscribe match output topic", logx.Field("symbol", v.Name), logx.Field("topic", topic))

		tickConsumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: config.Tick,
			Type:             pulsar.Exclusive,
		})
		if err != nil {
			logx.Severef("init tick consumer failed symbol=%s err=%v", v.Name, err)
			continue
		}

		tickerConsumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: config.Ticker,
			Type:             pulsar.Shared,
		})
		if err != nil {
			logx.Severef("init ticker consumer failed symbol=%s err=%v", v.Name, err)
			continue
		}

		klineConsumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: config.Kline,
			Type:             pulsar.Exclusive,
		})
		if err != nil {
			logx.Severef("init kline consumer failed symbol=%s err=%v", v.Name, err)
			continue
		}

		matchConsumers[v.Name] = &MatchSymbolConsumers{
			Tick:   tickConsumer,
			Ticker: tickerConsumer,
			Kline:  klineConsumer,
		}
	}

	mongoCli := c.MongoConf.MustNewClient()
	klineRepo := dao.NewKlineHistoryRepo(c.MongoConf.KlineColl(mongoCli))
	if err := klineRepo.EnsureIndex(context.Background()); err != nil {
		logx.Errorw("ensure kline_history index failed", logx.Field("err", err))
	}
	tickRepo := dao.NewTickRepo(c.MongoConf.TickColl(mongoCli))
	if err := tickRepo.EnsureIndex(context.Background()); err != nil {
		logx.Errorw("ensure tick index failed", logx.Field("err", err))
	}

	return &ServiceContext{
		Config:           c,
		TradeIdgen:       tradeIdgen,
		RedisClient:      redis.MustNewRedis(c.RedisConf),
		PulsarClient:     client,
		MatchConsumers:   matchConsumers,
		WsClient:         gpushPb.NewProxyClient(zrpc.MustNewClient(c.WsConf).Conn()),
		MongoCli:         mongoCli,
		KlineHistoryRepo: klineRepo,
		TickRepo:         tickRepo,
	}
}
