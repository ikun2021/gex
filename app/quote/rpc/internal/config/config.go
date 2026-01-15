package config

import (
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/pkg/etcd"
	commongorm "github.com/ikun2021/gex/common/pkg/gorm"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	GormConf         commongorm.GormConf
	RedisConf        redis.RedisConf
	WsConf           zrpc.RpcClientConf
	PulsarConfig     pulsar.PulsarConfig
	LoggerConfig     logger.Config
	Symbol           []models.Symbol
	Coin             []models.Coin
	EtcdRegisterConf etcd.EtcdRegisterConf `json:",optional"`
}

const (
	Ticker = "ticker_"
	Tick   = "tick_"
	Kline  = "kline_"
	Depth  = "depth_"
)
