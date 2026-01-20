package handler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao/quote/model"
	"github.com/ikun2021/gex/app/quote/rpc/internal/dao/quote/query"
	"github.com/ikun2021/gex/app/quote/rpc/internal/svc"
	"github.com/ikun2021/gex/common/models"
	"github.com/ikun2021/gex/common/proto/define"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	commonWs "github.com/ikun2021/gex/common/proto/ws"
	"github.com/ikun2021/gex/common/utils"
	gpush "github.com/luxun9527/gpush/proto"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm/clause"
	"time"
)

// KlineHandler 基于utc时间
type KlineHandler struct {
	//存储k线
	Klines []*model.MemoryKline
	//落库chan
	storeLatestKline chan model.StoreKline
	//发送
	sendChan chan model.MemoryKline
	//定时写入和发送的定时器
	ticker *time.Ticker
	//是否改变
	changed bool
	//提交方式
	consumer        pulsar.Consumer
	cron            *utils.WrapCron
	svcCtx          *svc.ServiceContext
	latestMessageID pulsar.MessageID
	latestMatchId   int64
	matchData       chan *model.MatchData
	symbolInfo      models.Symbol
}

func NewKlineHandler(svcCtx *svc.ServiceContext, consumer pulsar.Consumer, symbolInfo models.Symbol) *KlineHandler {
	klineHandler := &KlineHandler{
		storeLatestKline: make(chan model.StoreKline),
		sendChan:         make(chan model.MemoryKline),
		ticker:           time.NewTicker(300 * time.Millisecond),
		svcCtx:           svcCtx,
		matchData:        make(chan *model.MatchData, 10),
		symbolInfo:       symbolInfo,
	}

	wrapCron, err := utils.NewWrapCron("1 * * * * ?")
	if err != nil {
		logx.Severef("init cron failed %v", err)
	}
	klineHandler.cron = wrapCron
	klineHandler.start()
	return klineHandler
}

func (kl *KlineHandler) start() {
	kl.readInitData()
	kl.cron.Start()
	go kl.update()
	go kl.store()
	go kl.send()
}
func (kl *KlineHandler) Handle(msg pulsar.Message) {

	var m matchMq.MatchOutput
	if err := proto.Unmarshal(msg.Payload(), &m); err != nil {
		logx.Errorw("unmarshal match result failed", logger.ErrorField(err))
		if err := kl.consumer.Ack(msg); err != nil {
			logx.Errorw("consumer message failed", logger.ErrorField(err))
		}
		return
	}

	switch r := m.Result.(type) {
	case *matchMq.MatchOutput_MatchResult:
		logx.Debugw("receive match result data ", logx.Field("data", r))
		matchData := &model.MatchData{
			MessageID:  msg.ID(),
			MatchID:    cast.ToInt64(r.MatchResult.MatchId),
			MatchTime:  r.MatchResult.MatchTime / 1e9,
			Volume:     utils.NewFromString(r.MatchResult.QuoteAmount).Mul(utils.NewFromString("2")),
			Amount:     utils.NewFromString(r.MatchResult.BaseAmount).Mul(utils.NewFromString("2")),
			StartPrice: utils.NewFromString(r.MatchResult.BeginPrice),
			EndPrice:   utils.NewFromString(r.MatchResult.EndPrice),
			Low:        utils.NewFromString(r.MatchResult.LowPrice),
			High:       utils.NewFromString(r.MatchResult.HighPrice),
		}

		kl.matchData <- matchData
	}
}

func (kl *KlineHandler) readInitData() {
	klines := make([]*model.MemoryKline, 0, len(model.KlineTypes))
	for _, v := range model.KlineTypes {

		data, err := kl.svcCtx.RedisClient.Hget(define.Kline.WithParams(), kl.symbolInfo.Name+"_"+v.String())
		if err != nil {
			if errors.Is(err, redis.Nil) {
				kline := &model.MemoryKline{
					StartTime:   0,
					EndTime:     0,
					KlineType:   v,
					Amount:      utils.DecimalZeroMaxPrec,
					Volume:      utils.DecimalZeroMaxPrec,
					Open:        utils.DecimalZeroMaxPrec,
					High:        utils.DecimalZeroMaxPrec,
					Low:         utils.DecimalZeroMaxPrec,
					Close:       utils.DecimalZeroMaxPrec,
					Range:       "0",
					InitMatchID: 0,
				}
				klines = append(klines, kline)
				continue
			}
			logx.Severef("read init kline data failed err=%v", err)
		}
		var d model.RedisModel
		if err := json.Unmarshal([]byte(data), &d); err != nil {
			logx.Severef("unmarshal kline data failed err=%v", err)
		}

		kline := &model.MemoryKline{
			KlineType:   model.KlineType(d.KlineType),
			StartTime:   d.StartTime,
			EndTime:     d.EndTime,
			Amount:      utils.NewFromString(d.Volume),
			Volume:      utils.NewFromString(d.Amount),
			Open:        utils.NewFromString(d.Open),
			High:        utils.NewFromString(d.High),
			Low:         utils.NewFromString(d.Low),
			Close:       utils.NewFromString(d.Close),
			Range:       d.Range,
			InitMatchID: d.MatchID,
		}
		klines = append(klines, kline)
	}
	kl.Klines = klines

}
func (kl *KlineHandler) update() {
	for {
		select {
		case md := <-kl.matchData:
			kl.updateLatestKline(md, false)
			kl.changed = true
			kl.latestMessageID = md.MessageID
			kl.latestMatchId = md.MatchID
		case <-kl.ticker.C:
			if kl.changed {
				kl.snapshot()
			}
			kl.changed = false
			//定时在每分钟的开始输入一个成交量和成交额为0的订单。避免出现空数据。
		case <-kl.cron.C:
			kl.updateLatestKline(nil, true)
			kl.changed = true
		}
	}
}

// 存储历史k线和最新的k线
func (kl *KlineHandler) store() {
	klineDB := kl.svcCtx.GenDB.Kline
	for klineData := range kl.storeLatestKline {
		for {
			//存储历史k线
			if klineData.IsHistory {
				err := kl.svcCtx.GenDB.Transaction(func(tx *query.Query) error {
					for _, v := range klineData.Klines {
						mysqlData := v.CastToMysqlData(kl.symbolInfo)
						logx.Infow("store history kline data", logx.Field("data", mysqlData))
						onUpdate := map[string]interface{}{
							klineDB.Open.ColumnName().String():   mysqlData.Open,
							klineDB.High.ColumnName().String():   mysqlData.High,
							klineDB.Low.ColumnName().String():    mysqlData.Low,
							klineDB.Close.ColumnName().String():  mysqlData.Close,
							klineDB.Amount.ColumnName().String(): mysqlData.Amount,
							klineDB.Volume.ColumnName().String(): mysqlData.Volume,
							klineDB.Range.ColumnName().String():  mysqlData.Range,
						}
						if err := klineDB.WithContext(context.Background()).
							Clauses(clause.OnConflict{
								DoUpdates: clause.Assignments(onUpdate),
							}).
							Create(mysqlData); err != nil {
							logx.Errorw("store history kline data failed", logx.Field("data", mysqlData), logx.Field("err", err))
							return err
						}
					}

					return nil
				})
				if err != nil {
					logx.Errorf("store message to mysql failed err = %v message id %v", err, kl.latestMessageID)
					time.Sleep(time.Second * 3)
				}

			} else {
				//存储最新的k线
				if err := kl.svcCtx.GenDB.Transaction(func(tx *query.Query) error {
					for _, v := range klineData.Klines {
						data := v.CastToRedisData(kl.symbolInfo, klineData.MatchID)
						d, _ := json.Marshal(data)
						if err := kl.svcCtx.RedisClient.Hset(define.Kline.WithParams(), data.Symbol+"_"+v.KlineType.String(), string(d)); err != nil {
							logx.Errorw("update last kline failed", logger.ErrorField(err))
							return err
						}
					}

					if klineData.MessageID != nil {
						if err := kl.consumer.AckIDCumulative(kl.latestMessageID); err != nil {
							logx.Errorw("consumer message failed", logger.ErrorField(err), logx.Field("messageID", kl.latestMessageID))
							return err
						}
					}

					return nil
				}); err != nil {
					logx.Errorf("store last kline failed err=%v", err)
					time.Sleep(time.Second * 3)

				}
			}
		}

	}
}

func (kl *KlineHandler) snapshot() {
	latestKline := make([]*model.MemoryKline, 0, len(kl.Klines))
	for _, v := range kl.Klines {
		t := *v
		kl.sendChan <- t
		latestKline = append(latestKline, &t)

	}
	//定时存储最新的一根k线
	l := model.StoreKline{
		Klines:    latestKline,
		MessageID: kl.latestMessageID,
		MatchID:   kl.latestMatchId,
	}
	kl.storeLatestKline <- l
}
func (kl *KlineHandler) send() {
	for data := range kl.sendChan {
		msg := commonWs.Message[commonWs.Kline]{
			Topic:   commonWs.KlinePrefix.WithParam(kl.symbolInfo.Name) + "@" + data.KlineType.String(),
			Payload: data.CastToWsData(kl.symbolInfo),
		}
		if _, err := kl.svcCtx.WsClient.PushData(context.Background(), &gpush.Data{
			Topic: commonWs.KlinePrefix.WithParam(kl.symbolInfo.Name) + "@" + data.KlineType.String(),
			Data:  msg.ToBytes(),
		}); err != nil {
			logx.Errorw("push kline websocket data failed", logger.ErrorField(err), logx.Field("data", msg))
		}
	}
}

// 更新最新的k线
func (kl *KlineHandler) updateLatestKline(data *model.MatchData, isBegin bool) {
	logx.Debugw("update latest kline  data ", logx.Field("data", data))
	for _, klineData := range kl.Klines {
		logx.Debugw("before update ", logx.Field("klineData", klineData.CastToMysqlData(kl.symbolInfo)))

		// 小于初始化matchID的直接返回。
		if data != nil && data.MatchID != 0 && data.MatchID <= klineData.InitMatchID {
			return
		}
		//如果是每分钟的开始撮合用最新的价格计算
		if isBegin {
			//价格为零表示还没有成交。
			if klineData.Close.Equal(utils.DecimalZeroMaxPrec) {
				return
			}
			data = &model.MatchData{}
			data.MatchTime = time.Now().Unix()
			data.Amount = utils.DecimalZeroMaxPrec
			data.Volume = utils.DecimalZeroMaxPrec
			data.High = klineData.Close
			data.Low = klineData.Close
			data.StartPrice = klineData.Close
			data.EndPrice = klineData.Close
		}

		var (
			startTime,
			endTime int64
		)
		//修正交易时间为一个新的区间,如5分钟k线，交易时间为 06:23 则修改其为 05:00
		switch klineData.KlineType {
		case model.Week1:
			startTime = utils.BeginOfWeek(data.MatchTime)
			endTime = startTime + int64(klineData.KlineType.GetCycle())
		case model.Month1:
			startTime = utils.BeginOfMonth(data.MatchTime)
			endTime = utils.NextMonth(startTime)
		default:
			//去掉时间戳的余数
			startTime = data.MatchTime / int64(klineData.KlineType.GetCycle()) * int64(klineData.KlineType.GetCycle())
			endTime = startTime + int64(klineData.KlineType.GetCycle())
		}
		//初始化k线,这是项目启动的第一笔
		if klineData.Open.Equal(utils.DecimalZeroMaxPrec) {
			klineData.StartTime = startTime
			klineData.EndTime = endTime
			klineData.Open = data.StartPrice
			klineData.High = data.High
			klineData.Low = data.Low
			klineData.Close = data.EndPrice
			klineData.Amount = data.Amount
			klineData.Volume = data.Volume
			klineData.Range = "0"
			logx.Debugw("init kline after update ", logx.Field("klineData", klineData.CastToMysqlData(kl.symbolInfo)))
			continue
		}

		//如果k线在一个新的周期，要保存历史k线
		if startTime > klineData.StartTime && startTime > 0 {

			//存储历史k线
			//发送到发送和写的chan

			//极端情况需要补k线的情况。程序挂了，且这段时间没有成交，无法模拟成交，则需要补k线 程序10:22:05挂了，10:24:01分被拉起来且这段时间没有成交 要补10:23的k线
			internal := startTime - klineData.StartTime
			if internal > int64(klineData.KlineType.GetCycle()) {
				//确定要补几个
				c := internal/int64(klineData.KlineType.GetCycle()) - 1
				for i := int64(1); i <= c; i++ {
					k := klineData.Copy()
					k.Amount = utils.DecimalZeroMaxPrec
					k.Volume = utils.DecimalZeroMaxPrec
					k.StartTime = k.StartTime + int64(klineData.KlineType.GetCycle())*i
					k.EndTime = k.StartTime + int64(klineData.KlineType.GetCycle())
					sk := model.StoreKline{
						Klines:    []*model.MemoryKline{&k},
						IsHistory: true,
					}
					logx.Sloww("fix missing kline ", logx.Field("data", k.CastToMysqlData(kl.symbolInfo)))
					kl.storeLatestKline <- sk
				}
			}
			historyKline := klineData.Copy()
			//返回修改为最新的k线
			klineData.Open = data.StartPrice
			klineData.StartTime = startTime
			klineData.EndTime = endTime
			klineData.High = data.High
			klineData.Low = data.Low
			klineData.Close = data.EndPrice
			klineData.Amount = data.Amount
			klineData.Volume = data.Volume
			if !klineData.Open.Equal(utils.DecimalZeroMaxPrec) {
				klineData.Range = data.EndPrice.Sub(klineData.Open).Div(klineData.Open).Mul(utils.NewFromString("100")).StringFixedBank(3)
			}
			newKline := *klineData

			sk := model.StoreKline{
				Klines:    []*model.MemoryKline{&historyKline},
				IsHistory: true,
			}
			kl.sendChan <- historyKline
			kl.sendChan <- newKline
			kl.storeLatestKline <- sk
			logx.Debugw("generate new kline after update ", logx.Field("klineData", klineData.CastToMysqlData(kl.symbolInfo)))
			continue
		}
		//比较高低，累加成交量成交额
		klineData.StartTime = startTime
		klineData.EndTime = endTime
		//如果成交量为零说明这个k线是定时任务造成的。如果此时有真实成交造成的k线，则应该修改为真实k线
		if klineData.Amount.Equal(utils.DecimalZeroMaxPrec) {
			klineData.Open = data.StartPrice
			klineData.High = data.StartPrice
			klineData.Low = data.StartPrice
			klineData.Close = data.StartPrice
			klineData.Amount = klineData.Amount.Add(data.Amount)
			klineData.Volume = klineData.Volume.Add(data.Volume)
			klineData.Range = "0"
			continue
		}
		klineData.Amount = klineData.Amount.Add(data.Amount)
		klineData.Volume = klineData.Volume.Add(data.Volume)
		if data.High.GreaterThan(klineData.High) {
			klineData.High = data.High
		}
		if data.Low.LessThan(klineData.Low) {
			klineData.Low = data.Low
		}
		if !klineData.Open.Equal(utils.DecimalZeroMaxPrec) {
			klineData.Range = data.EndPrice.Sub(klineData.Open).Div(klineData.Open).Mul(utils.NewFromString("100")).StringFixedBank(3)
		}
		klineData.Close = data.EndPrice
		logx.Debugw("after update ", logx.Field("klineData", klineData.CastToMysqlData(kl.symbolInfo)))

	}
}
