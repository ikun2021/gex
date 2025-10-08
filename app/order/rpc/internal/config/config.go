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
	AccountRpcConf zrpc.RpcClientConf
	//dtm使用
	OrderRpcConf     zrpc.RpcClientConf
	DtmConf          zrpc.RpcClientConf
	PulsarConfig     pulsar.PulsarConfig
	LoggerConfig     logger.Config
	RedisConf        redis.RedisConf
	GormConf         commongorm.GormConf
	SymbolInfo       *define.SymbolInfo `json:",optional"`
	WsConf           zrpc.RpcClientConf
	Symbol           string
	SymbolEtcdConfig etcd.EtcdConfig
	EtcdRegisterConf etcd.EtcdRegisterConf `json:",optional"`
}
