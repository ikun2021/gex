package svc

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/dtm-labs/client/dtmgrpc/dtmgimp"
	"github.com/dtm-labs/client/dtmgrpc/dtmgpb"
	"github.com/ikun2021/gex/app/order/rpc/internal/config"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/query"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/ikun2021/gex/common/proto/define"
	ws "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/attributes"
	"strings"
	"time"

	pulsarConfig "github.com/ikun2021/gex/common/pkg/pulsar"
)

type ServiceContext struct {
	Config        *config.Config
	Query         *query.Query
	DtmClient     dtmgpb.DtmClient
	MatchConsumer pulsar.Consumer
	MatchProducer pulsar.Producer
	WsClient      ws.ProxyClient
	RedisClient   *redis.Redis
}

func NewServiceContext(c *config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	writer := logger.NewZapWriter(logger.GetZapLogger())
	logx.SetWriter(writer)
	logx.DisableStat()

	var symbolInfo define.SymbolInfo
	define.InitSymbolConfig(define.EtcdSymbolPrefix+c.Symbol, c.SymbolEtcdConfig, &symbolInfo)
	c.SymbolInfo = &symbolInfo
	logx.Infow("symbol config load ", logx.Field("symbol", symbolInfo))

	target, err := c.DtmConf.BuildTarget()
	if err != nil {
		logx.Severef("init dtm client failed %v", err)
		return nil
	}
	logx.Infof("dtm client init success %v", target)

	//注册到etcd orderapi使用
	d := strings.Split(c.RpcServerConf.ListenOn, ":")
	c.EtcdRegisterConf.Key += "/" + c.Symbol
	c.EtcdRegisterConf.Port = cast.ToInt32(d[1])
	c.EtcdRegisterConf.MataData = attributes.New("symbol", c.Symbol)
	etcd.Register(c.EtcdRegisterConf)
	//注册到etcd dtm使用
	c.Etcd.Key += "_" + c.Symbol

	c.OrderRpcConf.Etcd.Key += "_" + c.Symbol

	//使用交易对的Id作为workid
	var options = idgen.NewIdGeneratorOptions(uint16(c.SymbolInfo.SymbolID % 64))
	idgen.SetIdGenerator(options)

	c.SymbolInfo = &symbolInfo

	//初始化pulsar客户端
	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed %v", err)
	}
	topic := pulsarConfig.Topic{
		Tenant:    pulsarConfig.PublicTenant,
		Namespace: pulsarConfig.GexNamespace,
		Topic:     pulsarConfig.MatchSourceTopic + "_" + c.SymbolInfo.SymbolName,
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic.BuildTopic(),
		SendTimeout:     10 * time.Second,
		DisableBatching: true,
	})
	if err != nil {
		logx.Severef("init pulsar producer failed %v", err)
	}
	topic = pulsarConfig.Topic{
		Tenant:    pulsarConfig.PublicTenant,
		Namespace: pulsarConfig.GexNamespace,
		Topic:     pulsarConfig.MatchResultTopic + "_" + c.SymbolInfo.SymbolName,
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic.BuildTopic(),
		SubscriptionName: pulsarConfig.MatchResultOrderSub,
		Type:             pulsar.Shared,
	})
	if err != nil {
		logx.Severef("init pulsar consumer failed %v", err)
	}

	sc := &ServiceContext{
		Config:        c,
		Query:         query.Use(c.GormConf.MustNewGormClient()),
		DtmClient:     dtmgimp.MustGetDtmClient(target),
		MatchConsumer: consumer,
		MatchProducer: producer,
		WsClient:      ws.NewProxyClient(zrpc.MustNewClient(c.WsConf).Conn()),
		RedisClient:   redis.MustNewRedis(c.RedisConf),
	}
	return sc
}
