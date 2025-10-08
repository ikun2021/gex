package main

import (
	"flag"
	"github.com/ikun2021/gex/app/quotes/kline/rpc/internal/config"
	"github.com/ikun2021/gex/app/quotes/kline/rpc/internal/logic"
	"github.com/ikun2021/gex/app/quotes/kline/rpc/internal/server"
	"github.com/ikun2021/gex/app/quotes/kline/rpc/internal/svc"
	"github.com/ikun2021/gex/app/quotes/kline/rpc/pb"
	"github.com/ikun2021/gex/common/pkg/flagx"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "app/quotes/kline/rpc/etc/kline.yaml", "the config file")

func main() {
	flagx.Parse()
	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(&c)
	logic.InitKlineHandler(ctx)
	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		pb.RegisterKlineServiceServer(grpcServer, server.NewKlineServiceServer(ctx))
		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	// mustNewServer会将全局的logx的writer和日志等级重新设置。
	//不以go-zero的日志等级为准，所以将其设置为最低的
	logx.SetLevel(logx.DebugLevel)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.Infof("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
