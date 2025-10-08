package logic

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/model"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/query"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	matchMq "github.com/ikun2021/gex/common/proto/mq/match"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm/clause"
)

// HandleMatchResultLogic 结算
type HandleMatchResultLogic struct {
	svcCtx *svc.ServiceContext
}

// HandleMatchResult  结算，扣减用户资产
func (l *HandleMatchResultLogic) HandleMatchResult(result *matchMq.MatchResp_MatchResult, storeConsumedMessageId func() error) error {
	if len(result.MatchResult.MatchedRecord) == 0 {
		return nil
	}
	asset := l.svcCtx.Query.Asset
	if err := l.svcCtx.Query.Transaction(func(tx *query.Query) error {
		assetDo := tx.WithContext(context.Background()).Asset
		i := len(result.MatchResult.MatchedRecord) - 1
		//taker只更新一次
		//取taker基础币
		takerBaseCoin, err := assetDo.Select(asset.ID, asset.FrozenQty, asset.AvailableQty).
			Where(asset.CoinID.Eq(result.MatchResult.BaseCoinId), asset.UserID.
				Eq(result.MatchResult.MatchedRecord[i].Taker.Uid)).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Take()
		if err != nil {
			return err
		}
		logx.Debugw("[taker] before update get taker base coin ", logx.Field("data", takerBaseCoin))
		//取taker计价币
		takerQuoteCoin, err := assetDo.Select(asset.ID, asset.FrozenQty, asset.AvailableQty).
			Where(asset.CoinID.Eq(result.MatchResult.QuoteCoinId), asset.UserID.Eq(result.MatchResult.MatchedRecord[i].Taker.Uid)).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Take()
		if err != nil {
			return err
		}
		logx.Debugw("[taker] before update get taker quote coin ", logx.Field("data", takerBaseCoin))

		if result.MatchResult.TakerIsBuy {
			//taker 扣冻结计价币
			//usedAmount := result.MatchResult.MatchedRecord[i].Taker.UsedAmount
			//s := utils.NewFromStringMaxPrec(usedAmount).Add(utils.NewFromStringMaxPrec(result.MatchResult.Amount))
			//availableQty := utils.NewFromStringMaxPrec(takerQuoteCoin.AvailableQty).Add(s).String()
			//减去冻结计价币
			//冻结的金额 更新为订单的总金额减未成交的金额
			frozenQty := utils.NewFromStringMaxPrec(takerQuoteCoin.FrozenQty).Sub(utils.NewFromStringMaxPrec(result.MatchResult.MatchedRecord[i].Taker.UnFrozenAmount)).String()
			//加上 要解冻的金额,减成交的金额 解冻的金额要比成交的金额多
			a := utils.NewFromStringMaxPrec(result.MatchResult.MatchedRecord[i].Taker.UnFrozenAmount).Sub(utils.NewFromStringMaxPrec(result.MatchResult.MatchedRecord[i].Taker.FilledAmount))
			availableQty := utils.NewFromStringMaxPrec(takerQuoteCoin.AvailableQty).Add(a).String()
			if _, err := assetDo.
				Where(asset.ID.Eq(takerQuoteCoin.ID)).
				UpdateSimple(asset.FrozenQty.Value(frozenQty), asset.AvailableQty.Value(availableQty)); err != nil {
				return err
			}
			//taker 加可用基础币
			qty := utils.NewFromStringMaxPrec(takerBaseCoin.AvailableQty).Add(utils.NewFromStringMaxPrec(result.MatchResult.Qty))
			if _, err := assetDo.
				Where(asset.ID.Eq(takerBaseCoin.ID)).
				Update(asset.AvailableQty, qty); err != nil {
				return err
			}

		} else {
			//taker 减冻结基础币
			qty := utils.NewFromStringMaxPrec(takerBaseCoin.FrozenQty).Sub(utils.NewFromStringMaxPrec(result.MatchResult.Qty))
			if _, err := assetDo.
				Where(asset.ID.Eq(takerBaseCoin.ID)).
				Update(asset.FrozenQty, qty); err != nil {
				return err
			}
			//taker 加可用计价币
			amount := utils.NewFromStringMaxPrec(takerQuoteCoin.AvailableQty).Add(utils.NewFromStringMaxPrec(result.MatchResult.Amount))
			if _, err := assetDo.
				Where(asset.ID.Eq(takerQuoteCoin.ID)).
				Update(asset.AvailableQty, amount); err != nil {
				return err
			}
		}
		for _, v := range result.MatchResult.MatchedRecord {
			//计算都先查出来算
			//taker买的话 taker 加可用基础币，减冻结计价币。 maker为卖 减冻结基础币，加可用计价币
			//taker卖的话 taker 加可用计价币，减冻结基础币。maker为买 减冻结计价币，加可用基础币
			var (
				makerBaseCoin  *model.Asset
				makerQuoteCoin *model.Asset
			)

			makerBaseCoin, err = assetDo.Select(asset.ID, asset.FrozenQty, asset.AvailableQty).
				Where(asset.CoinID.Eq(result.MatchResult.BaseCoinId), asset.UserID.Eq(v.Maker.Uid)).
				Clauses(clause.Locking{Strength: "UPDATE"}).
				Take()
			if err != nil {
				return err
			}
			makerQuoteCoin, err = assetDo.Select(asset.ID, asset.FrozenQty, asset.AvailableQty).
				Where(asset.CoinID.Eq(result.MatchResult.QuoteCoinId), asset.UserID.Eq(v.Maker.Uid)).Clauses(clause.Locking{Strength: "UPDATE"}).
				Take()
			if err != nil {
				return err
			}
			if result.MatchResult.TakerIsBuy {
				//maker 减冻结基础币
				makerBaseCoin.FrozenQty = utils.NewFromStringMaxPrec(makerBaseCoin.FrozenQty).Sub(utils.NewFromStringMaxPrec(v.Qty)).String()
				if _, err := assetDo.
					Where(asset.ID.Eq(makerBaseCoin.ID)).
					Update(asset.FrozenQty, makerBaseCoin.FrozenQty); err != nil {
					return err
				}
				//maker 加可用计价币
				makerQuoteCoin.AvailableQty = utils.NewFromStringMaxPrec(makerQuoteCoin.AvailableQty).Add(utils.NewFromStringMaxPrec(v.Amount)).String()
				if _, err := assetDo.
					Where(asset.ID.Eq(makerQuoteCoin.ID)).
					Update(asset.AvailableQty, makerQuoteCoin.AvailableQty); err != nil {
					return err
				}
			} else {
				//maker 扣冻结计价币
				makerQuoteCoin.FrozenQty = utils.NewFromStringMaxPrec(makerQuoteCoin.FrozenQty).Sub(utils.NewFromStringMaxPrec(v.Amount)).String()
				if _, err := assetDo.
					Where(asset.ID.Eq(makerQuoteCoin.ID)).
					Update(asset.FrozenQty, makerQuoteCoin.FrozenQty); err != nil {
					return err
				}
				//maker 加可用基础币
				makerBaseCoin.AvailableQty = utils.NewFromStringMaxPrec(makerBaseCoin.AvailableQty).Add(utils.NewFromStringMaxPrec(v.Qty)).String()
				if _, err := assetDo.
					Where(asset.ID.Eq(makerBaseCoin.ID)).
					Update(asset.AvailableQty, makerBaseCoin.AvailableQty); err != nil {
					return err
				}
			}
		}
		if err := storeConsumedMessageId(); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.Errorw("HandleMatchResultLogic HandleCancelOrder err", logger.ErrorField(err))
		return err
	}

	return nil
}

func NewHandleMatchResultLogic(svcCtx *svc.ServiceContext) *HandleMatchResultLogic {
	return &HandleMatchResultLogic{
		svcCtx: svcCtx,
	}
}

// HandleCancelOrder 取消订单解冻
func (l *HandleMatchResultLogic) HandleCancelOrder(cancelResp *matchMq.MatchResp_Cancel, storeConsumedMessageId func() error) error {
	asset := l.svcCtx.Query.Asset
	return l.svcCtx.Query.Transaction(func(tx *query.Query) error {
		assetDetail, err := tx.Asset.WithContext(context.Background()).
			Where(asset.UserID.Eq(cancelResp.Cancel.Uid), asset.CoinID.Eq(cancelResp.Cancel.CoinId)).
			Select(asset.ID, asset.FrozenQty, asset.AvailableQty).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			First()
		if err != nil {
			logx.Errorw("[cancel ] query user asset  failed", logger.ErrorField(err))
			return err
		}
		logx.Debugw("[cancel] before update", logx.Field("assetDetail", assetDetail), logx.Field("Qty", cancelResp.Cancel.Qty))
		q := utils.NewFromStringMaxPrec(cancelResp.Cancel.Qty)
		frozenQty := utils.NewFromStringMaxPrec(assetDetail.FrozenQty).Sub(q)
		availableQty := utils.NewFromStringMaxPrec(assetDetail.AvailableQty).Add(q)
		logx.Debugw("[cancel] after update", logx.Field("frozenQty", frozenQty), logx.Field("availableQty", availableQty))
		_, err = tx.Asset.WithContext(context.Background()).
			Where(asset.ID.Eq(assetDetail.ID)).
			UpdateSimple(asset.AvailableQty.Value(availableQty.String()), asset.FrozenQty.Value(frozenQty.String()))
		if err != nil {
			logx.Errorw("[cancel ] update user asset  failed", logger.ErrorField(err))
			return err
		}
		if err := storeConsumedMessageId(); err != nil {
			return err
		}
		return nil
	})

}
