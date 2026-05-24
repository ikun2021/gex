package svc

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/account/rpc/internal/config"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/mongodao"
	"github.com/ikun2021/gex/common/defines"
	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"
	"github.com/redis/go-redis/v9"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/mongo"
)

type ServiceContext struct {
	Config            config.Config
	MatchConsumerList []pulsar.Consumer
	JwtClient         *utils.JWT
	UserRepo          *mongodao.UserRepo
	RedisCli          *redis.Client
	MatchProducers    map[string]pulsar.Producer
	MongoCli          *mongo.Client
	OrderFinalRepo    *mongodao.OrderFinalRepo
	MatchTradeRepo    *mongodao.MatchTradeRepo
	SettleArchive     *mongodao.SettleArchiveStore
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()

	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed %v", err)
	}
	consumers := make([]pulsar.Consumer, 0, 10)

	m := make(map[string]pulsar.Producer)
	for _, v := range c.Symbol {
		topic := pulsarConfig.Topic{
			Tenant:    pulsarConfig.PublicTenant,
			Namespace: pulsarConfig.GexNamespace,
			Topic:     defines.MatchTopicInputPrefix + v.Name,
		}
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic:           topic.BuildTopic(),
			SendTimeout:     10 * time.Second,
			DisableBatching: true,
		})
		if err != nil {
			logx.Severef("create producer failed %v", err)
		}
		consumerTopic := pulsarConfig.Topic{
			Tenant:    pulsarConfig.PublicTenant,
			Namespace: pulsarConfig.GexNamespace,
			Topic:     defines.MatchTopicOutputPrefix + v.Name,
		}
		logx.Infof("create consumer topic %s", consumerTopic.BuildTopic())
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            consumerTopic.BuildTopic(),
			SubscriptionName: "account_" + v.Name,
		})
		if err != nil {
			logx.Severef("init match consumer error:%v", err)
			continue
		}
		consumers = append(consumers, consumer)

		m[v.Name] = producer
	}

	idgen.SetIdGenerator(idgen.NewIdGeneratorOptions(2))

	mongoCli := c.MongoConf.MustNewClient()
	orderFinalRepo := mongodao.NewOrderFinalRepo(c.MongoConf.OrderFinalColl(mongoCli))
	if err := orderFinalRepo.EnsureIndex(context.Background()); err != nil {
		logx.Errorw("ensure order_final index failed", logx.Field("err", err))
	}
	matchTradeRepo := mongodao.NewMatchTradeRepo(c.MongoConf.MatchTradeColl(mongoCli))
	if err := matchTradeRepo.EnsureIndex(context.Background()); err != nil {
		logx.Errorw("ensure match_trade index failed", logx.Field("err", err))
	}
	userRepo := mongodao.NewUserRepo(c.MongoConf.UserColl(mongoCli))
	if err := userRepo.EnsureIndex(context.Background()); err != nil {
		logx.Errorw("ensure user index failed", logx.Field("err", err))
	}

	sc := &ServiceContext{
		Config:            c,
		MatchConsumerList: consumers,
		JwtClient:         utils.NewJWT(&c.JwtConf),
		UserRepo:          userRepo,
		MatchProducers:    m,
		MongoCli:          mongoCli,
		OrderFinalRepo:    orderFinalRepo,
		MatchTradeRepo:    matchTradeRepo,
		SettleArchive:     mongodao.NewSettleArchiveStore(mongoCli, matchTradeRepo, orderFinalRepo),
		RedisCli: redis.NewClient(&redis.Options{
			Addr:     c.RedisConf.Host,
			Password: c.RedisConf.Pass,
		}),
	}
	return sc
}
