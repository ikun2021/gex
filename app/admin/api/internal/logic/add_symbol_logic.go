package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/admin/api/internal/dao/admin/model"
	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type AddSymbolLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAddSymbolLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddSymbolLogic {
	return &AddSymbolLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *AddSymbolLogic) AddSymbol(req *types.AddSymbolReq) (resp *types.AddSymbolResp, err error) {

	var (
		coin   = l.svcCtx.AdminQuery.Coin
		symbol = l.svcCtx.AdminQuery.Symbol
	)
	baseCoinInfo, err := coin.WithContext(l.ctx).Where(coin.CoinID.Eq(req.BaseCoinID)).Take()
	if err != nil {
		logx.Errorf("AddSymbol find BaseCoinID err: %v", err)
		return nil, err
	}
	quoteCoinInfo, err := coin.WithContext(l.ctx).Where(coin.CoinID.Eq(req.QuoteCoinID)).Take()
	if err != nil {
		logx.Errorf("AddSymbol find QuoteCoinID err: %v", err)
		return nil, err
	}
	symbolName := baseCoinInfo.CoinName + "_" + quoteCoinInfo.CoinName
	c := &model.Symbol{
		SymbolName:    symbolName,
		SymbolID:      req.SymbolId,
		BaseCoinID:    uint32(req.BaseCoinID),
		BaseCoinName:  baseCoinInfo.CoinName,
		BaseCoinPrec:  baseCoinInfo.Prec,
		QuoteCoinID:   uint32(req.QuoteCoinID),
		QuoteCoinName: quoteCoinInfo.CoinName,
		QuoteCoinPrec: quoteCoinInfo.Prec,
	}
	if err := symbol.WithContext(l.ctx).Create(c); err != nil {
		if errors.Is(gorm.ErrDuplicatedKey, err) {
			return nil, errs.DuplicateDataErr
		}
		logx.Errorw("create symbol failed", logx.Field("err", err))
		return nil, err
	}

	return &types.AddSymbolResp{}, nil
}
