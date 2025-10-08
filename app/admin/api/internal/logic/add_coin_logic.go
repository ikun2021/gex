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

type AddCoinLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAddCoinLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddCoinLogic {
	return &AddCoinLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *AddCoinLogic) AddCoin(req *types.AddCoinReq) (resp *types.AddCoinResp, err error) {

	coin := l.svcCtx.AdminQuery.Coin
	c := &model.Coin{
		CoinName: req.CoinName,
		Prec:     req.Prec,
		CoinID:   req.CoinId,
	}
	if err := coin.WithContext(l.ctx).Create(c); err != nil {
		if errors.Is(gorm.ErrDuplicatedKey, err) {
			return nil, errs.DuplicateDataErr
		}
		logx.Errorw("create coin failed", logx.Field("err", err))
		return &types.AddCoinResp{}, err
	}

	return
}
