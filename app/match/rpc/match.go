package main

import (
	"flag"
	"github.com/ikun2021/gex/app/match/rpc/internal/bootstrap"
	"github.com/ikun2021/gex/app/match/rpc/internal/config"
	"github.com/ikun2021/gex/app/match/rpc/internal/server"
	"github.com/ikun2021/gex/app/match/rpc/internal/svc"
	"github.com/ikun2021/gex/app/match/rpc/pb"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "app/match/rpc/etc/match.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	//初始化配置
	ctx := svc.NewServiceContext(&c)
	//初始化
	bootstrap.Start(ctx)
	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		pb.RegisterMatchServiceServer(grpcServer, server.NewMatchServiceServer(ctx))
		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})

	defer s.Stop()
	logx.SetLevel(logx.DebugLevel)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.Infof("Starting rpc server at %s...", c.ListenOn)
	s.Start()
}
