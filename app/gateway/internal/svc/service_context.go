package svc

import (
	"github.com/ikun2021/gex/app/gateway/internal/config"
	"github.com/ikun2021/gex/app/gateway/internal/middleware"
	"github.com/zeromicro/go-zero/rest"
)

type ServiceContext struct {
	Config config.Config
	Auth   rest.Middleware
}

func NewServiceContext(c config.Config) *ServiceContext {
	return &ServiceContext{
		Config: c,
		Auth:   middleware.NewAuthMiddleware().Handle,
	}
}
