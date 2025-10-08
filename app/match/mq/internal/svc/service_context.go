package svc

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/mq/internal/config"
	"github.com/ikun2021/gex/app/match/mq/internal/dao/model"
	"github.com/ikun2021/gex/app/match/mq/internal/dao/query"
	"github.com/ikun2021/gex/app/order/rpc/orderservice"
	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/proto/define"
	gpushPb "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	MatchConsumer pulsar.Consumer
	Config        config.Config
	OrderClient   orderservice.OrderService
	Query         *query.Query
	RedisClient   *redis.Redis
	WsClient      gpushPb.ProxyClient
	MatchDataChan chan *model.MatchData
	SymbolInfo    *define.SymbolInfo
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()

	var symbolInfo define.SymbolInfo
	define.InitSymbolConfig(define.EtcdSymbolPrefix+c.Symbol, c.SymbolEtcdConfig, &symbolInfo)
	logx.Infof("init symbol config symbolInfo %+v", &symbolInfo)

	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed err %v", err)
	}
	topic := pulsarConfig.Topic{
		Tenant:    pulsarConfig.PublicTenant,
		Namespace: pulsarConfig.GexNamespace,
		Topic:     pulsarConfig.MatchResultTopic + "_" + c.Symbol,
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic.BuildTopic(),
		SubscriptionName: pulsarConfig.MatchResultMatchSub,
		Type:             pulsar.Shared,
	})
	if err != nil {
		logx.Severef("init pulsar consumer failed %v", logger.ErrorField(err))
	}
	logx.Infof("init pulsar consumer success")
	sc := &ServiceContext{
		MatchConsumer: consumer,
		Config:        c,
		Query:         query.Use(c.GormConf.MustNewGormClient()),
		WsClient:      gpushPb.NewProxyClient(zrpc.MustNewClient(c.WsConf).Conn()),
		RedisClient:   redis.MustNewRedis(c.RedisConf),
		MatchDataChan: make(chan *model.MatchData),
		SymbolInfo:    &symbolInfo,
	}
	return sc
}
