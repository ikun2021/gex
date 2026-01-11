package config

import (
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	PulsarConfig     pulsar.PulsarConfig
	LoggerConfig     logger.Config
	Symbol           []models.Symbol
	Coin             []models.Coin
	OrderRpcConf     zrpc.RpcClientConf
	RedisConf        redis.RedisConf
	EtcdRegisterConf etcd.EtcdRegisterConf `json:",optional"`
}
