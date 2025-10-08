package confx

import (
	"context"
	"github.com/ikun2021/gex/common/pkg/etcd"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

//考虑到以后的运维使用动态配置。

type ConfigPrefix string

const (
	Prefix              = "config"
	Kline  ConfigPrefix = "kline"
	Ticker ConfigPrefix = "ticker"
	Match  ConfigPrefix = "match"
	Order  ConfigPrefix = "order"
)

func (c ConfigPrefix) BuildKey(symbol string) string {
	return Prefix + "/" + string(c) + "/" + symbol
}

type Op int8

const (
	InitLoad Op = iota + 1
	WatchChange
)

type ConfigCustomFuncOption struct {
	op Op
	f  any
}

func WithCustomInitLoadFunc(f func(kvs []*mvccpb.KeyValue, target any)) ConfigCustomFuncOption {
	return ConfigCustomFuncOption{
		op: InitLoad,
		f:  f,
	}
}

func WithCustomWatchFunc(f func(evs []*clientv3.Event, target any)) ConfigCustomFuncOption {
	return ConfigCustomFuncOption{
		op: WatchChange,
		f:  f,
	}
}

func WithDefaultInitLoadFunc() ConfigCustomFuncOption {
	return ConfigCustomFuncOption{
		op: InitLoad,
		f:  DefaultLoadFunc,
	}
}

var (
	DefaultLoadFunc = func(kvs []*mvccpb.KeyValue, target any) {
		data := kvs[0].Value
		if err := conf.LoadFromYamlBytes(data, target); err != nil {
			log.Panicf("load and parse config from etcd fail err = %v", err)
		}
	}
	DefaultWatchOnConfigChange = func(evs []*clientv3.Event, target any) {

		for _, ev := range evs {
			//switch ev.Type {
			//case mvccpb.PUT: //修改或者新增
			//case mvccpb.DELETE: //删除
			//}
			logx.Sloww(" config  has changed", logx.Field("data", string(ev.Kv.Value)))
			if err := conf.LoadFromYamlBytes(ev.Kv.Value, target); err != nil {
				logx.Errorw("load and parse config from etcd fail err = %v", logger.ErrorField(err))
			}
		}

	}
)

// MustLoadFromEtcd 从etcd中加载配置，有状态的服务从etcd中获取
func MustLoadFromEtcd(key string, etcdConfig etcd.EtcdConfig, target any, ops ...ConfigCustomFuncOption) {

	client, err := etcdConfig.NewEtcdClient()
	if err != nil {
		logx.Severef("init etcd client failed err %v", err)
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFunc()
	response, err := client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		logx.Severef("get service  config failed err %v", err)
	}
	var (
		initLoadFunc        = DefaultLoadFunc
		watchOnConfigChange func(evs []*clientv3.Event, target any)
	)
	for _, v := range ops {
		switch v.op {
		case InitLoad:
			initLoadFunc = v.f.(func(kvs []*mvccpb.KeyValue, target any))
		case WatchChange:
			watchOnConfigChange = v.f.(func(evs []*clientv3.Event, target any))
		}
	}

	initLoadFunc(response.Kvs, target)
	logx.Infow("load config from etcd ", logx.Field("detail", target))
	if watchOnConfigChange != nil {
		go WatchConfig(key, target, client, watchOnConfigChange)
	}

}
func WatchConfig(key string, target any, cli *clientv3.Client, f func(evs []*clientv3.Event, target any)) {
	rch := cli.Watch(context.Background(), key, clientv3.WithPrefix())
	for resp := range rch {
		logx.Sloww("config changed", logx.Field("key", key), logx.Field("data", resp.Events))
		f(resp.Events, target)
	}
}
