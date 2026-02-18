package main

import (
	"flag"

	"github.com/ikun2021/gex/app/match/internal/config"
	"github.com/ikun2021/gex/app/match/internal/consumer"
	"github.com/ikun2021/gex/app/match/internal/svc"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "app/match/etc/match.dev.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	//初始化配置
	ctx := svc.NewServiceContext(c)
	consumer.InitMatchConsumer(ctx)
	logx.SetLevel(logx.DebugLevel)
	logx.SetWriter(logger.NewZapWriter(logger.GetZapLogger()))
	logx.Infof("start match service ")
	select {}
}
