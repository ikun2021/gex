package svc

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/account/rpc/internal/config"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/query"
	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"time"
)

type ServiceContext struct {
	Config            config.Config
	Query             *query.Query
	MatchConsumerList []pulsar.Consumer
	JwtClient         *utils.JWT
	RedisCli          *redis.Client
	MatchProducer     pulsar.Producer
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

	topic := pulsarConfig.Topic{
		Tenant:    pulsarConfig.PublicTenant,
		Namespace: pulsarConfig.GexNamespace,
		//	Topic:     pulsarConfig.MatchSourceTopic + "_" + c.SymbolInfo.SymbolName,
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic.BuildTopic(),
		SendTimeout:     10 * time.Second,
		DisableBatching: true,
	})

	sc := &ServiceContext{
		Config:            c,
		MatchConsumerList: consumers,
		JwtClient:         utils.NewJWT(),
		MatchProducer:     producer,
	}
	return sc
}
