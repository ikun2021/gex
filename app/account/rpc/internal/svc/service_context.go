package svc

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/account/rpc/internal/config"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/query"
	"github.com/ikun2021/gex/common/defines"
	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"
	"github.com/redis/go-redis/v9"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
)

type ServiceContext struct {
	Config            config.Config
	Query             *query.Query
	MatchConsumerList []pulsar.Consumer
	JwtClient         *utils.JWT
	RedisCli          *redis.Client
	MatchProducers    map[string]pulsar.Producer
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
	sc := &ServiceContext{
		Config:            c,
		MatchConsumerList: consumers,
		JwtClient:         utils.NewJWT(),
		MatchProducers:    m,
		RedisCli: redis.NewClient(&redis.Options{
			Addr:     c.RedisConf.Host,
			Password: c.RedisConf.Pass,
		}),
	}
	return sc
}
