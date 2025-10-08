package svc

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/rpc/internal/config"
	"github.com/ikun2021/gex/app/match/rpc/internal/dao/query"
	"github.com/ikun2021/gex/app/match/rpc/internal/engine"
	"github.com/ikun2021/gex/app/order/rpc/orderservice"
	"github.com/ikun2021/gex/common/pkg/etcd"
	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/proto/define"
	ws "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"strings"
	"time"
)

type ServiceContext struct {
	MatchConsumer      pulsar.Consumer
	Config             *config.Config
	MatchEngine        *engine.MatchEngine
	OrderClient        orderservice.OrderService
	Query              *query.Query
	RedisClient        *redis.Redis
	InitOrderPrimaryID int64
}

func NewServiceContext(c *config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()

	var symbolInfo define.SymbolInfo
	define.InitSymbolConfig(define.EtcdSymbolPrefix+c.Symbol, c.SymbolEtcdConfig, &symbolInfo)
	logx.Infof("init symbol config symbolInfo %+v", &symbolInfo)
	c.SymbolInfo = &symbolInfo

	//注册到etcd
	d := strings.Split(c.RpcServerConf.ListenOn, ":")
	c.EtcdRegisterConf.Key += "/" + c.Symbol
	c.EtcdRegisterConf.Port = cast.ToInt32(d[1])
	c.EtcdRegisterConf.MataData = attributes.New("symbol", c.Symbol)
	etcd.Register(c.EtcdRegisterConf)

	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed err %v", err)
	}
	topic := pulsarConfig.Topic{
		Tenant:    pulsarConfig.PublicTenant,
		Namespace: pulsarConfig.GexNamespace,
		Topic:     pulsarConfig.MatchResultTopic + "_" + c.Symbol,
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic.BuildTopic(),
		SendTimeout:     10 * time.Second,
		DisableBatching: true,
	})
	if err != nil {
		logx.Severef("init pulsar producer failed %v", logger.ErrorField(err))
	}
	topic = pulsarConfig.Topic{
		Tenant:    pulsarConfig.PublicTenant,
		Namespace: pulsarConfig.GexNamespace,
		Topic:     pulsarConfig.MatchSourceTopic + "_" + c.Symbol,
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic.BuildTopic(),
		SubscriptionName: pulsarConfig.MatchSourceSub,
		Type:             pulsar.Shared,
	})
	if err != nil {
		logx.Severef("init pulsar consumer failed err = %v", err)
	}
	//自定义负载均衡策略
	var clientOpts []zrpc.ClientOption
	serviceConfig := grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"symbol_lb"}`)

	//自定义resolver
	etcdConfig := etcd.EtcdConfig{Endpoints: c.OrderRpcConf.Etcd.Hosts}
	etcdCli := etcdConfig.MustNewEtcdClient()
	etcdResolver, err := resolver.NewBuilder(etcdCli)
	if err != nil {
		logx.Severef("NewBuilder error: %v", err)
	}
	r := grpc.WithResolvers(etcdResolver)

	clientOpts = append(clientOpts, zrpc.WithDialOption(r), zrpc.WithDialOption(serviceConfig))

	//使用交易对的Id作为workid
	var options = idgen.NewIdGeneratorOptions(uint16(c.SymbolInfo.SymbolID) % 64)
	idgen.SetIdGenerator(options)
	sc := &ServiceContext{
		MatchConsumer: consumer,
		Config:        c,
		OrderClient:   orderservice.NewOrderService(zrpc.MustNewClient(c.OrderRpcConf, clientOpts...)),
		MatchEngine:   engine.NewMatchEngine(c, producer, ws.NewProxyClient(zrpc.MustNewClient(c.WsConf).Conn())),
		Query:         query.Use(c.GormConf.MustNewGormClient()),
		RedisClient:   redis.MustNewRedis(c.RedisConf),
	}
	return sc
}
