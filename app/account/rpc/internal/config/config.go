package config

import (
	"github.com/ikun2021/gex/common/pkg/etcd"
	commongorm "github.com/ikun2021/gex/common/pkg/gorm"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	GormConf         commongorm.GormConf
	LoggerConfig     logger.Config
	PulsarConfig     pulsar.PulsarConfig
	RedisConf        redis.RedisConf
	SymbolEtcdConfig etcd.EtcdConfig
}
