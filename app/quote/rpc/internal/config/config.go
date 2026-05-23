package config

import (
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/pkg/etcd"
	commonmongo "github.com/ikun2021/gex/common/pkg/mongo"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	MongoConf        commonmongo.Conf
	RedisConf        redis.RedisConf
	WsConf           zrpc.RpcClientConf
	PulsarConfig     pulsar.PulsarConfig
	LoggerConfig     logger.Config
	Symbol           []models.Symbol
	Coin             []models.Coin
	EtcdRegisterConf etcd.EtcdRegisterConf `json:",optional"`
}

const (
	Ticker = "ticker"
	Tick   = "tick"
	Kline  = "kline"
	Depth  = "depth"
)
