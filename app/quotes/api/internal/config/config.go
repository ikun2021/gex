package config

import (
	"github.com/ikun2021/gex/common/pkg/etcd"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	KlineRpcConf     zrpc.RpcClientConf
	MatchRpcConf     zrpc.RpcClientConf
	LoggerConfig     logger.Config
	LanguageEtcdConf etcd.EtcdConfig
	SymbolEtcdConfig etcd.EtcdConfig
}
