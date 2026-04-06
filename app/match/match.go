package main

import (
	"flag"

	"github.com/ikun2021/gex/app/match/internal/config"
	"github.com/ikun2021/gex/app/match/internal/handler"
	"github.com/ikun2021/gex/app/match/internal/server"
	"github.com/ikun2021/gex/app/match/internal/svc"
	"github.com/ikun2021/gex/app/match/pb"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "app/match/etc/match.dev.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(c)

	handler.InitMatchHandler(ctx)
	logx.SetLevel(logx.DebugLevel)

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		pb.RegisterMatchServiceServer(grpcServer, server.NewMatchServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.Infof("start match service ")
	s.Start()
}
