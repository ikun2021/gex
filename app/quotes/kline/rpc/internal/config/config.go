package config

import (
	"github.com/ikun2021/gex/common/pkg/etcd"
	commongorm "github.com/ikun2021/gex/common/pkg/gorm"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/proto/define"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	GormConf         commongorm.GormConf
	RedisConf        redis.RedisConf
	WsConf           zrpc.RpcClientConf
	PulsarConfig     pulsar.PulsarConfig
	LoggerConfig     logger.Config
	Symbol           string
	SymbolEtcdConfig etcd.EtcdConfig
	SymbolInfo       *define.SymbolInfo    `json:",optional"`
	EtcdRegisterConf etcd.EtcdRegisterConf `json:",optional"`
}
