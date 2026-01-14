package engine

import (
	"github.com/ikun2021/gex/app/match/internal/config"
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/shopspring/decimal"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
	"reflect"
	"github.com/agiledragon/gomonkey/v2"

	"testing"
)

func TestMatch(t *testing.T) {
	var me = NewMatchEngine(models.Symbol{
		Name:        "",
		BaseCoinId:  0,
		BaseCoin:    "",
		QuoteCoin:   "",
		QuoteCoinId: 0,
		Id:          0,
	}, config.Config{
		PulsarConfig:     pulsar.PulsarConfig{},
		Symbol:           nil,
		Coin:             nil,
		OrderRpcConf:     zrpc.RpcClientConf{},
		RedisConf:        redis.RedisConf{},
		EtcdRegisterConf: etcd.EtcdRegisterConf{},
	}, nil)

	// ApplyMethod 参数：
	// 1. reflect.TypeOf(receiver): 获取接收者的类型（如果是指针接收者，需要传指针）
	// 2. "FetchData": 方法名字符串
	// 3. func(...): 桩函数。注意：桩函数的第一个参数必须是接收者（这里是 *User），后面才是原方法的参数
	patches := gomonkey.ApplyMethod(reflect.TypeOf(me), "SendResult", func(_ *MatchEngine, matchMsg *MatchOutputMessage) (string, error) {
		return "mocked data", nil
	})
	defer patches.Reset()

	me.HandleOrder(&Order{
		OrderID:             "",
		SequenceId:          0,
		CreateTime:          0,
		IsCancel:            false,
		Uid:                 0,
		Price:               decimal.Decimal{},
		BaseAmount:          decimal.Decimal{},
		OrderType:           0,
		QuoteAmount:         decimal.Decimal{},
		Side:                0,
		OrderStatus:         0,
		UnfilledBaseAmount:  decimal.Decimal{},
		FilledBaseAmount:    decimal.Decimal{},
		UnfilledQuoteAmount: decimal.Decimal{},
		FilledQuoteAmount:   decimal.Decimal{},
	})

}
