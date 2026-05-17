package config

import (
	"github.com/ikun2021/gex/common/models"
	commongorm "github.com/ikun2021/gex/common/pkg/gorm"
	commonmongo "github.com/ikun2021/gex/common/pkg/mongo"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	GormConf     commongorm.GormConf
	MongoConf    commonmongo.Conf
	LoggerConfig logger.Config
	PulsarConfig pulsar.PulsarConfig
	RedisConf    redis.RedisConf
	Symbol       []models.Symbol
	Coin         []models.Coin
}
