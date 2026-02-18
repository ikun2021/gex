package svc

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/internal/config"
	ws "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config       config.Config
	PulsarClient pulsar.Client
	RedisClient  *redis.Redis
	WsClient     ws.ProxyClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()

	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed %v", err)
	}
	idgen.SetIdGenerator(idgen.NewIdGeneratorOptions(1))

	redisClient := redis.MustNewRedis(c.RedisConf)

	return &ServiceContext{
		Config:       c,
		PulsarClient: client,
		RedisClient:  redisClient,
		WsClient:     ws.NewProxyClient(zrpc.MustNewClient(c.WsConf).Conn()),
	}
}
