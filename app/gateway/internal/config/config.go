package config

import (
	"github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	LoggerConfig   zlog.Config
	AccountRpcConf zrpc.RpcClientConf
	LangPath       string
}
