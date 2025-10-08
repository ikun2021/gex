package main

import (
	"flag"
	"github.com/ikun2021/gex/app/match/mq/internal/config"
	"github.com/ikun2021/gex/app/match/mq/internal/consumer"
	"github.com/ikun2021/gex/app/match/mq/internal/logic"
	"github.com/ikun2021/gex/app/match/mq/internal/svc"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/zeromicro/go-zero/core/conf"
)

var configFile = flag.String("f", "app/match/mq/etc/match.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(c)
	consumer.InitConsumer(ctx)
	logic.InitHandler(ctx)
	logx.SetLevel(logx.DebugLevel)
	logx.Info("server start")
	select {}
}
