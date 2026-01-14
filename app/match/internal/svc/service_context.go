package svc

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/match/internal/config"
	"github.com/zeromicro/go-zero/core/logx"
)

type ServiceContext struct {
	Config       config.Config
	PulsarClient pulsar.Client
}

func NewServiceContext(c config.Config) *ServiceContext {
	client, err := c.PulsarConfig.BuildClient()
	if err != nil {
		logx.Severef("init pulsar client failed %v", err)
	}

	return &ServiceContext{
		Config:       c,
		PulsarClient: client,
	}
}
