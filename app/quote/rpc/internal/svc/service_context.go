package svc

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/config"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao/quote/query"
	"github.com/yitter/idgenerator-go/idgen"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"

	logger "github.com/ikun2021/zlog"
	gpushPb "github.com/luxun9527/gpush/proto"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config           *config.Config
	DB               *gorm.DB
	GenDB            *query.Query
	MongoCli         *mongo.Client
	KlineHistoryRepo *dao.KlineHistoryRepo
	TickRepo         *dao.TickRepo
	RedisClient      *redis.Redis
	PulsarClient     pulsar.Client
	TradeIdgen       *idgen.DefaultIdGenerator
	WsClient         gpushPb.ProxyClient
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

	mongoCli := c.MongoConf.MustNewClient()
	klineRepo := dao.NewKlineHistoryRepo(c.MongoConf.KlineColl(mongoCli))
	if err := klineRepo.EnsureIndex(context.Background()); err != nil {
		logx.Errorw("ensure kline_history index failed", logx.Field("err", err))
	}
	tickRepo := dao.NewTickRepo(c.MongoConf.TickColl(mongoCli))
	if err := tickRepo.EnsureIndex(context.Background()); err != nil {
		logx.Errorw("ensure tick index failed", logx.Field("err", err))
	}

	sc := &ServiceContext{
		Config:           c,
		TradeIdgen:       tradeIdgen,
		RedisClient:      redis.MustNewRedis(c.RedisConf),
		PulsarClient:     client,
		WsClient:         gpushPb.NewProxyClient(zrpc.MustNewClient(c.WsConf).Conn()),
		DB:               db,
		GenDB:            genDB,
		MongoCli:         mongoCli,
		KlineHistoryRepo: klineRepo,
		TickRepo:         tickRepo,
	}
	return sc
}
