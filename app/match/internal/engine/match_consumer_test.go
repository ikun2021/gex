package engine

import (
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/ikun2021/gex/app/match/internal/config"
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/ikun2021/gex/common/pkg/pulsar"
	"github.com/ikun2021/gex/common/proto/enum"
	"github.com/shopspring/decimal"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/stringx"
	"reflect"
	"time"

	"testing"
)

func TestMatch(t *testing.T) {
	var me = NewMatchEngine(models.Symbol{
		Name:        "USDT_BTC",
		BaseCoinId:  2,
		BaseCoin:    "BTC",
		QuoteCoin:   "USDT",
		QuoteCoinId: 1,
		Id:          1,
	}, config.Config{
		PulsarConfig: pulsar.PulsarConfig{},
		Symbol:       nil,
		Coin: []models.Coin{{
			Name:      "USDT",
			Id:        1,
			Precision: 6,
		}, {
			Name:      "BTC",
			Id:        2,
			Precision: 6,
		}},
		EtcdRegisterConf: etcd.EtcdRegisterConf{},
	}, nil)

	// ApplyMethod 参数：
	// 1. reflect.TypeOf(receiver): 获取接收者的类型（如果是指针接收者，需要传指针）
	// 2. "FetchData": 方法名字符串
	// 3. func(...): 桩函数。注意：桩函数的第一个参数必须是接收者（这里是 *User），后面才是原方法的参数
	patches := gomonkey.ApplyMethod(reflect.TypeOf(me), "SendResult", func(_ *MatchEngine, matchMsg *MatchOutputMessage) {
		matchMsg.Dump()
	})
	defer patches.Reset()
	idgen.SetIdGenerator(idgen.NewIdGeneratorOptions(1))
	me.HandleOrder(&Order{
		OrderID:             stringx.Rand(),
		SequenceId:          1,
		CreateTime:          time.Now().UnixNano(),
		IsCancel:            false,
		Uid:                 1,
		Price:               decimal.New(1, 1), //10
		BaseAmount:          decimal.New(1, 1),
		OrderType:           enum.OrderType_LO,
		QuoteAmount:         decimal.New(1, 1),
		Side:                enum.Side_Buy,
		OrderStatus:         enum.OrderStatus_NewCreated,
		UnfilledBaseAmount:  decimal.New(1, 1),
		FilledBaseAmount:    decimal.Decimal{},
		UnfilledQuoteAmount: decimal.New(1, 1),
		FilledQuoteAmount:   decimal.Decimal{},
	})
	me.HandleOrder(&Order{
		OrderID:             stringx.Rand(),
		SequenceId:          1,
		CreateTime:          time.Now().UnixNano(),
		IsCancel:            false,
		Uid:                 1,
		Price:               decimal.New(1, 1), //10
		BaseAmount:          decimal.New(1, 1),
		OrderType:           enum.OrderType_LO,
		QuoteAmount:         decimal.New(1, 1),
		Side:                enum.Side_Sell,
		OrderStatus:         enum.OrderStatus_NewCreated,
		UnfilledBaseAmount:  decimal.New(1, 1),
		FilledBaseAmount:    decimal.Decimal{},
		UnfilledQuoteAmount: decimal.New(1, 1),
		FilledQuoteAmount:   decimal.Decimal{},
	})
	me.dump()

}
func TestMatchCancel(t *testing.T) {
	var me = NewMatchEngine(models.Symbol{
		Name:        "USDT_BTC",
		BaseCoinId:  2,
		BaseCoin:    "BTC",
		QuoteCoin:   "USDT",
		QuoteCoinId: 1,
		Id:          1,
	}, config.Config{
		PulsarConfig: pulsar.PulsarConfig{},
		Symbol:       nil,
		Coin: []models.Coin{{
			Name:      "USDT",
			Id:        1,
			Precision: 6,
		}, {
			Name:      "BTC",
			Id:        2,
			Precision: 6,
		}},
		EtcdRegisterConf: etcd.EtcdRegisterConf{},
	}, nil)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(me), "SendResult", func(_ *MatchEngine, matchMsg *MatchOutputMessage) {
		matchMsg.Dump()
	})
	defer patches.Reset()
	idgen.SetIdGenerator(idgen.NewIdGeneratorOptions(1))
	me.HandleOrder(&Order{
		OrderID:             stringx.Rand(),
		SequenceId:          1,
		CreateTime:          time.Now().UnixNano(),
		IsCancel:            false,
		Uid:                 1,
		Price:               decimal.New(1, 1), //10
		BaseAmount:          decimal.New(1, 1),
		OrderType:           enum.OrderType_LO,
		QuoteAmount:         decimal.New(1, 1),
		Side:                enum.Side_Buy,
		OrderStatus:         enum.OrderStatus_NewCreated,
		UnfilledBaseAmount:  decimal.New(1, 1),
		FilledBaseAmount:    decimal.Decimal{},
		UnfilledQuoteAmount: decimal.New(1, 1),
		FilledQuoteAmount:   decimal.Decimal{},
	})
	me.HandleOrder(&Order{
		OrderID:             stringx.Rand(),
		SequenceId:          2,
		CreateTime:          time.Now().UnixNano(),
		IsCancel:            false,
		Uid:                 1,
		Price:               decimal.New(1, 1), //10
		BaseAmount:          decimal.New(1, 1),
		OrderType:           enum.OrderType_LO,
		QuoteAmount:         decimal.New(1, 1),
		Side:                enum.Side_Buy,
		OrderStatus:         enum.OrderStatus_NewCreated,
		UnfilledBaseAmount:  decimal.New(1, 1),
		FilledBaseAmount:    decimal.Decimal{},
		UnfilledQuoteAmount: decimal.New(1, 1),
		FilledQuoteAmount:   decimal.Decimal{},
	})
	fmt.Printf("处理订单后:\n")
	me.dump()

}
