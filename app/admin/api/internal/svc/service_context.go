package svc

import (
	"github.com/ikun2021/gex/app/admin/api/internal/config"
	adminQuery "github.com/ikun2021/gex/app/admin/api/internal/dao/admin/query"
	matchQuery "github.com/ikun2021/gex/app/admin/api/internal/dao/match/query"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ServiceContext struct {
	Config     config.Config
	EtcdCli    *clientv3.Client
	JwtClient  *utils.JWT
	AdminQuery *adminQuery.Query
	MatchQuery *matchQuery.Query
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()
	cli, err := c.EtcdConf.NewEtcdClient()
	errs.InitTranslatorFromEtcd(c.LanguageEtcdConf)
	if err != nil {
		logx.Severef("init etcd client failed %v", err)
	}
	return &ServiceContext{
		Config:     c,
		EtcdCli:    cli,
		JwtClient:  utils.NewJWT(),
		AdminQuery: adminQuery.Use(c.AdminGormConf.MustNewGormClient()),
		MatchQuery: matchQuery.Use(c.MatchGormConf.MustNewGormClient()),
	}
}
