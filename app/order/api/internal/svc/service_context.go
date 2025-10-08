package svc

import (
	"github.com/ikun2021/gex/app/account/rpc/accountservice"
	"github.com/ikun2021/gex/app/match/rpc/matchservice"
	matchpb "github.com/ikun2021/gex/app/match/rpc/pb"
	"github.com/ikun2021/gex/app/order/api/internal/config"
	"github.com/ikun2021/gex/app/order/api/internal/middleware"
	"github.com/ikun2021/gex/app/order/rpc/orderservice"
	orderpb "github.com/ikun2021/gex/app/order/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/pkg/confx"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/ikun2021/gex/common/proto/define"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
	"sync"
)

type ServiceContext struct {
	Config           config.Config
	OrderClient      orderpb.OrderServiceClient
	MatchClient      matchpb.MatchServiceClient
	Auth             rest.Middleware
	AccountRpcClient accountservice.AccountService
	Symbols          *sync.Map
}

func NewServiceContext(c config.Config) *ServiceContext {
	logger.InitDefaultLogger(&c.LoggerConfig)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.DisableStat()
	errs.InitTranslatorFromEtcd(c.LanguageEtcdConf)

	var symbolConfig sync.Map
	// 从etcd中取出交易对配置。
	confx.MustLoadFromEtcd(define.EtcdSymbolPrefix, c.SymbolEtcdConfig, &symbolConfig, confx.WithCustomInitLoadFunc(func(kvs []*mvccpb.KeyValue, target any) {
		for _, v := range kvs {
			var s define.SymbolInfo
			if err := yaml.Unmarshal(v.Value, &s); err != nil {
				logx.Severef("get symbol config failed symbolInfo = %v", define.EtcdSymbolPrefix)
			}
			s.QuoteCoinPrec.Store(s.QuoteCoinPrecValue)
			s.BaseCoinPrec.Store(s.BaseCoinPrecValue)
			symbolConfig.Store(s.SymbolName, &s)
			logx.Infof("symbol config loaded symbolConfig %+v", &symbolConfig)

		}
	}), confx.WithCustomWatchFunc(func(evs []*clientv3.Event, target any) {
		for _, v := range evs {
			switch v.Type {
			case mvccpb.PUT: //修改或者新增
				var s define.SymbolInfo
				if err := yaml.Unmarshal(v.Kv.Value, &s); err != nil {
					logx.Errorf("get symbol config failed symbolInfo =%v", s)
					continue
				}
				logx.Slowf("symbol config changed symbolConfig %+v", &s)
				s.QuoteCoinPrec.Store(s.QuoteCoinPrecValue)
				s.BaseCoinPrec.Store(s.BaseCoinPrecValue)
				symbolConfig.Store(s.SymbolName, &s)
				logx.Slowf("symbol config changed after added symbolConfig %+v", &symbolConfig)

			case mvccpb.DELETE: //删除
				var s define.SymbolInfo
				if err := yaml.Unmarshal(v.Kv.Value, &s); err != nil {
					logx.Errorf("get symbol config failed symbolInfo =%v", s)
					continue
				}
				logx.Slowf("delete symbol config changed symbolConfig %+v", &s)
				symbolConfig.Delete(s.SymbolName)
				logx.Slowf("symbol config changed after added symbolConfig %+v", &symbolConfig)
			}
		}
	}))
	//自定义负载均衡策略
	var clientOpts []zrpc.ClientOption
	serviceConfig := grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"symbol_lb"}`)

	//自定义resolver
	etcdConfig := etcd.EtcdConfig{Endpoints: c.MatchRpcConf.Etcd.Hosts}
	etcdCli := etcdConfig.MustNewEtcdClient()
	etcdResolver, err := resolver.NewBuilder(etcdCli)
	if err != nil {
		logx.Severef("NewBuilder error: %v", err)
	}
	r := grpc.WithResolvers(etcdResolver)

	clientOpts = append(clientOpts, zrpc.WithDialOption(r), zrpc.WithDialOption(serviceConfig))

	accountRpcClient := accountservice.NewAccountService(zrpc.MustNewClient(c.AccountRpcConf))
	return &ServiceContext{
		Config:           c,
		Auth:             middleware.NewAuthMiddleware(accountRpcClient).Handle,
		MatchClient:      matchservice.NewMatchService(zrpc.MustNewClient(c.MatchRpcConf, clientOpts...)),
		OrderClient:      orderservice.NewOrderService(zrpc.MustNewClient(c.OrderRpcConf, clientOpts...)),
		AccountRpcClient: accountRpcClient,
		Symbols:          &symbolConfig,
	}
}
