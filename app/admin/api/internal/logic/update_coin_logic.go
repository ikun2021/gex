package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/admin/api/internal/dao/admin/model"
	"github.com/ikun2021/gex/app/admin/api/internal/dao/admin/query"
	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"gorm.io/gorm"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateCoinLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateCoinLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateCoinLogic {
	return &UpdateCoinLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateCoinLogic) UpdateCoin(req *types.UpdateCoinReq) (resp *types.Empty, err error) {
	// todo: add your logic here and delete this line
	symbol := l.svcCtx.AdminQuery.Symbol
	c := &model.Coin{
		CoinName: req.CoinName,
		Prec:     req.Prec,
		ID:       req.ID,
	}
	if err := l.svcCtx.AdminQuery.Transaction(func(tx *query.Query) error {
		if _, err := tx.WithContext(l.ctx).Coin.Updates(c); err != nil {
			return err
		}
		//更新交易对中的配置
		if _, err := tx.WithContext(l.ctx).Symbol.
			Where(symbol.BaseCoinID.Eq(req.ID)).
			UpdateColumnSimple(symbol.BaseCoinPrec.Value(req.Prec)); err != nil {
			return err
		}

		if _, err := tx.WithContext(l.ctx).Symbol.
			Where(symbol.QuoteCoinID.Eq(req.ID)).
			UpdateColumnSimple(symbol.QuoteCoinPrec.Value(req.Prec)); err != nil {
			return err
		}

		return nil
	}); err != nil {
		if errors.Is(gorm.ErrDuplicatedKey, err) {
			return nil, errs.DuplicateDataErr
		}
		return nil, err
	}
	return
}
