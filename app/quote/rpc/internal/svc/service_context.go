package svc

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/config"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao/quote/query"
	"github.com/yitter/idgenerator-go/idgen"
	"gorm.io/gorm"

	logger "github.com/ikun2021/zlog"
	gpushPb "github.com/luxun9527/gpush/proto"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config       *config.Config
	DB           *gorm.DB
	GenDB        *query.Query
	RedisClient  *redis.Redis
	PulsarClient pulsar.Client
	TradeIdgen   *idgen.DefaultIdGenerator
	WsClient     gpushPb.ProxyClient
}

func NewServiceContext(c *config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()

	idgen.SetIdGenerator(idgen.NewIdGeneratorOptions(1))

	tradeIdgen := idgen.NewDefaultIdGenerator(idgen.NewIdGeneratorOptions(1))

	//初始化pulsar客户端
	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed %v", err)
	}
	db := c.GormConf.MustNewGormClient()
	genDB := query.Use(db)

	sc := &ServiceContext{
		Config:       c,
		TradeIdgen:   tradeIdgen,
		RedisClient:  redis.MustNewRedis(c.RedisConf),
		PulsarClient: client,
		WsClient:     gpushPb.NewProxyClient(zrpc.MustNewClient(c.WsConf).Conn()),
		DB:           db,
		GenDB:        genDB,
	}
	return sc
}
