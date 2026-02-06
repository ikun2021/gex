package svc

import (
	"github.com/ikun2021/gex/app/account/rpc/client/accountservice"
	"github.com/ikun2021/gex/app/account/rpc/client/orderservice"
	"github.com/ikun2021/gex/app/gateway/internal/config"
	"github.com/ikun2021/gex/app/gateway/internal/middleware"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config     config.Config
	Auth       rest.Middleware
	AccountRpc accountservice.AccountService
	OrderRpc   orderservice.OrderService
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()
	cli := accountservice.NewAccountService(zrpc.MustNewClient(c.AccountRpcConf))
	orderRpc := orderservice.NewOrderService(zrpc.MustNewClient(c.AccountRpcConf))
	return &ServiceContext{
		Config:     c,
		AccountRpc: cli,
		OrderRpc:   orderRpc,
		Auth:       middleware.NewAuthMiddleware().Handle,
	}
}
