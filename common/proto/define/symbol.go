package define

import (
	"github.com/ikun2021/gex/common/pkg/confx"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/zeromicro/go-zero/core/logx"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v3"
	"sync"
)

const (
	EtcdSymbolPrefix = "Symbol/"
	EtcdCoinPrefix   = "Coin/"
)

func InitSymbolConfig(key string, etcdConfig etcd.EtcdConfig, symbolInfo *SymbolInfo) {
	confx.MustLoadFromEtcd(key, etcdConfig, symbolInfo, confx.WithCustomInitLoadFunc(func(kvs []*mvccpb.KeyValue, target any) {
		for _, v := range kvs {
			if len(v.Value) == 0 {
				logx.Severef("load  symbol config failed key = %v", key)
			}
			if err := yaml.Unmarshal(v.Value, symbolInfo); err != nil {
				logx.Severef("get symbol config failed symbolInfo = %v", key)
			}
			if symbolInfo.BaseCoinPrecValue <= 0 || symbolInfo.QuoteCoinPrecValue <= 0 {
				logx.Severef("base coin prec quote coin prec hava a invalid QuoteCoinPrecValue = %v BaseCoinPrecValue =%v ", symbolInfo.QuoteCoinPrecValue, symbolInfo.BaseCoinPrecValue)

			}
			symbolInfo.BaseCoinPrec.Store(symbolInfo.BaseCoinPrecValue)
			symbolInfo.QuoteCoinPrec.Store(symbolInfo.QuoteCoinPrecValue)

		}
	}), confx.WithCustomWatchFunc(func(evs []*clientv3.Event, target any) {
		for _, v := range evs {
			switch v.Type {
			case mvccpb.PUT: //修改或者新增
				if err := yaml.Unmarshal(v.Kv.Value, symbolInfo); err != nil {
					logx.Errorf("get symbol config failed symbolInfo =%v", key)
				}
				symbolInfo.QuoteCoinPrec.Store(symbolInfo.QuoteCoinPrecValue)
				symbolInfo.BaseCoinPrec.Store(symbolInfo.BaseCoinPrecValue)
			case mvccpb.DELETE: //删除
				logx.Sloww("warn symbol config deleted")
			}

		}
	}))
}

type SymbolInfo struct {
	SymbolName         string
	SymbolID           int32
	BaseCoinName       string
	BaseCoinID         int32
	QuoteCoinName      string
	QuoteCoinID        int32
	BaseCoinPrecValue  int32        `yaml:"baseCoinPrec"`
	QuoteCoinPrecValue int32        `yaml:"quoteCoinPrec"`
	BaseCoinPrec       atomic.Int32 `yaml:"-"`
	QuoteCoinPrec      atomic.Int32 `yaml:"-"`
}

type CoinInfo struct {
	CoinID   int32
	CoinName string
	Prec     int32
}

type SymbolCoinConfig[Key string | int32, Value *SymbolInfo | *CoinInfo] map[Key]Value

func (s SymbolCoinConfig[Key, Value]) CastToSyncMap() *sync.Map {
	m := &sync.Map{}
	for k, v := range s {
		m.Store(k, v)
	}
	return m
}
