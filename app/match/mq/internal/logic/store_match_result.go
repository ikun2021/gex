package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/match/mq/internal/dao/model"
	"github.com/ikun2021/gex/app/match/mq/internal/dao/query"
	"github.com/ikun2021/gex/app/match/mq/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type StoreMatchResultLogic struct {
	svcCtx *svc.ServiceContext
}

func NewStoreMatchResultLogic(svcCtx *svc.ServiceContext) *StoreMatchResultLogic {
	return &StoreMatchResultLogic{
		svcCtx: svcCtx,
	}
}

func (l *StoreMatchResultLogic) StoreMatchResult(result *matchMq.MatchResp_MatchResult, storeMsgID func() error) error {
	if err := l.svcCtx.Query.Transaction(func(tx *query.Query) error {

		for _, v := range result.MatchResult.MatchedRecord {
			f := int32(2)
			if result.MatchResult.TakerIsBuy {
				f = 1
			}
			mr := &model.MatchedOrder{
				MatchID:      result.MatchResult.MatchId,
				SymbolID:     result.MatchResult.SymbolId,
				SymbolName:   result.MatchResult.SymbolName,
				TakerOrderID: v.Taker.OrderId,
				MakerOrderID: v.Maker.OrderId,
				MatchSubID:   v.MatchSubId,
				Price:        v.Price,
				Qty:          v.Qty,
				Amount:       v.Amount,
				MatchTime:    result.MatchResult.MatchTime,
				TakerIsBuyer: f,
			}
			//重复消费也问题不大
			if err := tx.WithContext(context.Background()).MatchedOrder.Create(mr); err != nil {
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					logx.Sloww("matched order already exists has Duplicated key", logx.Field("data", mr))
					continue
				}
				return err

			}
		}
		if err := storeMsgID(); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.Errorw("write match result to mysql failed", logx.Field("err", err))
		return err
	}
	return nil
}
