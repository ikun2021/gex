package main

import (
	"flag"

	"github.com/ikun2021/gex/app/quote/rpc/internal/config"
	"github.com/ikun2021/gex/app/quote/rpc/internal/consumer"
	"github.com/ikun2021/gex/app/quote/rpc/internal/server"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quote/rpc/pb"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "app/quote/rpc/etc/quote.dev.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(&c)
	consumer.InitConsumer(ctx)
	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		pb.RegisterQuoteServiceServer(grpcServer, server.NewQuoteServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.Infof("starting quote server at: %s", c.ListenOn)
	s.Start()
}
