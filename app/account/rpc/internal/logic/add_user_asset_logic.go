package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/model"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/query"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type AddUserAssetLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAddUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddUserAssetLogic {
	return &AddUserAssetLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 增加用户资产
func (l *AddUserAssetLogic) AddUserAsset(in *pb.AddUserAssetReq) (*pb.Empty, error) {
	asset := l.svcCtx.Query.Asset

	c, ok := l.svcCtx.Coins.Load(in.CoinName)
	if !ok {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "coin not found")
	}
	coinInfo := c.(*define.CoinInfo)
	if err := l.svcCtx.Query.Transaction(func(tx *query.Query) error {
		userAsset, err := tx.Asset.WithContext(l.ctx).
			Where(asset.CoinID.Eq(coinInfo.CoinID), asset.UserID.Eq(in.Uid)).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Take()
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				m := &model.Asset{
					UserID:       in.Uid,
					CoinID:       coinInfo.CoinID,
					CoinName:     coinInfo.CoinName,
					AvailableQty: in.Qty,
					FrozenQty:    "0",
					CreatedAt:    time.Now().Unix(),
					UpdatedAt:    time.Now().Unix(),
				}
				if err := tx.Asset.WithContext(l.ctx).Create(m); err != nil {
					logx.Errorw("AddUserAsset create asset record failed", logger.ErrorField(err))
					return errs.Internal
				}
				return nil
			}
			logx.Errorw("AddUserAsset query asset record failed", logger.ErrorField(err))
			return errs.Internal
		}
		s := utils.NewFromStringMaxPrec(userAsset.AvailableQty).Add(utils.NewFromStringMaxPrec(in.Qty)).String()
		userAsset.AvailableQty = s
		if _, err := tx.Asset.WithContext(l.ctx).Updates(userAsset); err != nil {
			logx.Errorw("AddUserAsset update asset record failed", logger.ErrorField(err))
			return errs.Internal
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}
