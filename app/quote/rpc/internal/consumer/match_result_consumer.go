package consumer

import (
	"context"

	"github.com/ikun2021/gex/app/quote/rpc/internal/handler"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/models"
	logger "github.com/ikun2021/zlog"
	"github.com/zeromicro/go-zero/core/logx"
)

// InitConsumer 启动各交易对 tick / ticker / kline 消费循环（消费者已在 ServiceContext 中创建）。
func InitConsumer(sc *svc.ServiceContext) {
	for _, sym := range sc.Config.Symbol {
		consumers, ok := sc.MatchConsumers[sym.Name]
		if !ok || consumers == nil {
			logx.Errorw("match consumers not found for symbol", logx.Field("symbol", sym.Name))
			continue
		}
		startTickConsumer(sc, sym, consumers)
		startTickerConsumer(sc, sym, consumers)
		startKlineConsumer(sc, sym, consumers)
	}
}

func startTickConsumer(sc *svc.ServiceContext, s models.Symbol, c *svc.MatchSymbolConsumers) {
	go func() {
		tickHandle := handler.NewTickHandle(sc, c.Tick, s)
		for {
			message, err := c.Tick.Receive(context.Background())
			if err != nil {
				logx.Errorw("tick consumer receive failed", logx.Field("symbol", s.Name), logger.ErrorField(err))
				continue
			}
			tickHandle.Handle(message)
		}
	}()
}

func startTickerConsumer(sc *svc.ServiceContext, s models.Symbol, c *svc.MatchSymbolConsumers) {
	go func() {
		tickerHandler := handler.NewTickerHandler(sc, c.Ticker, s)
		for {
			message, err := c.Ticker.Receive(context.Background())
			if err != nil {
				logx.Errorw("ticker consumer receive failed", logx.Field("symbol", s.Name), logger.ErrorField(err))
				continue
			}
			tickerHandler.Handle(message)
		}
	}()
}

func startKlineConsumer(sc *svc.ServiceContext, s models.Symbol, c *svc.MatchSymbolConsumers) {
	go func() {
		klineHandler := handler.NewKlineHandler(sc, c.Kline, s)
		for {
			message, err := c.Kline.Receive(context.Background())
			if err != nil {
				logx.Errorw("kline consumer receive failed", logx.Field("symbol", s.Name), logger.ErrorField(err))
				continue
			}
			klineHandler.Handle(message)
		}
	}()
}
