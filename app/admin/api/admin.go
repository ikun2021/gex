package main

import (
	"flag"
	"fmt"
	"github.com/ikun2021/gex/common/pkg/validatorx"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"

	"github.com/ikun2021/gex/app/admin/api/internal/config"
	"github.com/ikun2021/gex/app/admin/api/internal/handler"
	"github.com/ikun2021/gex/app/admin/api/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "app/admin/api/etc/admin.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	httpx.SetValidator(validatorx.NewValidator())

	server := rest.MustNewServer(c.RestConf, rest.WithCors())
	defer server.Stop()

	ctx := svc.NewServiceContext(c)
	logx.SetLevel(logx.DebugLevel)
	handler.RegisterHandlers(server, ctx)
	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
