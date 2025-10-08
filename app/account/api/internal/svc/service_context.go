package svc

import (
	"github.com/ikun2021/gex/app/account/api/internal/config"
	"github.com/ikun2021/gex/app/account/api/internal/middleware"
	"github.com/ikun2021/gex/app/account/rpc/accountservice"
	"github.com/ikun2021/gex/common/errs"
	logger "github.com/luxun9527/zlog"
	"github.com/mojocn/base64Captcha"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"time"
)

type ServiceContext struct {
	Config           config.Config
	AccountRpcClient accountservice.AccountService
	Auth             rest.Middleware
	CaptchaStore     base64Captcha.Store
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()
	sc := &ServiceContext{
		Config:           c,
		Auth:             middleware.NewAuthMiddleware(accountservice.NewAccountService(zrpc.MustNewClient(c.AccountRpcConf))).Handle,
		AccountRpcClient: accountservice.NewAccountService(zrpc.MustNewClient(c.AccountRpcConf)),
		CaptchaStore:     base64Captcha.NewMemoryStore(10000, time.Minute*3),
	}
	errs.InitTranslatorFromEtcd(c.LanguageEtcdConf)

	return sc
}
