package config

import (
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	PulsarConfig pulsar.PulsarConfig
	zrpc.RpcServerConf
	LoggerConfig     logger.Config
	Symbol           []models.Symbol
	Coin             []models.Coin
	WsConf           zrpc.RpcClientConf
	EtcdRegisterConf etcd.EtcdRegisterConf `json:",optional"`
	RedisConf        redis.RedisConf
}
