package config

import (
	"github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	LoggerConfig   zlog.Config
	AccountRpcConf zrpc.RpcClientConf
	MatchRpcConf   zrpc.RpcClientConf
	LangPath       string
}
