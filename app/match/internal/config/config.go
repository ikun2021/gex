package config

import (
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type Config struct {
	PulsarConfig     pulsar.PulsarConfig
	LoggerConfig     logger.Config
	Symbol           []models.Symbol
	Coin             []models.Coin
	EtcdRegisterConf etcd.EtcdRegisterConf `json:",optional"`
	RedisConf        redis.RedisConf
}