package svc

import (
	"github.com/ikun2021/gex/app/account/rpc/client/accountservice"
	"github.com/ikun2021/gex/app/account/rpc/client/orderservice"
	"github.com/ikun2021/gex/app/gateway/internal/config"
	"github.com/ikun2021/gex/app/gateway/internal/middleware"
	"github.com/ikun2021/gex/app/match/matchservice"
	"github.com/ikun2021/gex/common/errs"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config     config.Config
	Auth       rest.Middleware
	AccountRpc accountservice.AccountService
	OrderRpc   orderservice.OrderService
	MatchRpc   matchservice.MatchService
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()
	cli := accountservice.NewAccountService(zrpc.MustNewClient(c.AccountRpcConf))
	orderRpc := orderservice.NewOrderService(zrpc.MustNewClient(c.AccountRpcConf))
	matchRpc := matchservice.NewMatchService(zrpc.MustNewClient(c.MatchRpcConf))
	translator, err := errs.NewTranslator(c.LangPath)
	if err != nil {
		logx.Severef("init translator failed %v", err)
	}
	errs.SetDefaultTranslator(translator)
	return &ServiceContext{
		Config:     c,
		AccountRpc: cli,
		OrderRpc:   orderRpc,
		MatchRpc:   matchRpc,
		Auth:       middleware.NewAuthMiddleware().Handle,
	}
}
