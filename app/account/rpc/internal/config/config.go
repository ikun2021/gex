package config

import (
	"github.com/ikun2021/gex/common/models"
	commonmongo "github.com/ikun2021/gex/common/pkg/mongo"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	MongoConf    commonmongo.Conf
	LoggerConfig logger.Config
	PulsarConfig pulsar.PulsarConfig
	RedisConf    redis.RedisConf
	Symbol       []models.Symbol
	Coin         []models.Coin
}
