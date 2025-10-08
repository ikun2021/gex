package logic

import (
	"context"
	"database/sql"
	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/model"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type UnFreezeUserAssetLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnFreezeUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnFreezeUserAssetLogic {
	return &UnFreezeUserAssetLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 解冻用户资产
func (l *UnFreezeUserAssetLogic) UnFreezeUserAsset(in *pb.FreezeUserAssetReq) (*pb.Empty, error) {
	asset := l.svcCtx.Query.Asset
	barrier, err := dtmgrpc.BarrierFromGrpc(l.ctx)
	if err != nil {
		logx.Errorw("BarrierFromGrpc db failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.DtmErr)
	}

	underlyingDB := asset.WithContext(l.ctx).UnderlyingDB()
	var ma model.Asset
	querySql := underlyingDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Select(asset.ID.ColumnName().String(), asset.AvailableQty.ColumnName().String(), asset.FrozenQty.ColumnName().String()).
			Clauses(asset.UserID.Eq(in.Uid)).
			Clauses(asset.CoinID.Eq(in.CoinId)).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Take(&ma)
	})

	db, err := underlyingDB.DB()
	if err != nil {
		logx.Errorw("get db failed", logger.ErrorField(err))
		return nil, errs.ExecSqlFailed
	}

	logx.Infow("req", logx.Field("data", in))
	if err := barrier.CallWithDB(db, func(tx *sql.Tx) error {
		var userAsset model.Asset
		row := tx.QueryRow(querySql)
		if row.Err() != nil {
			return row.Err()
		}
		if err := row.Scan(&userAsset.ID, &userAsset.AvailableQty, &userAsset.FrozenQty); err != nil {
			return err
		}
		if userAsset.ID == 0 {
			return errs.UserNotFound
		}

		frozenQty := utils.NewFromStringMaxPrec(userAsset.FrozenQty)
		qty := utils.NewFromStringMaxPrec(in.Qty)
		if frozenQty.LessThan(qty) {
			return errs.AmountInsufficient
		}

		userAsset.FrozenQty = utils.NewFromStringMaxPrec(userAsset.FrozenQty).Sub(qty).String()
		userAsset.AvailableQty = frozenQty.Add(qty).String()

		updateSql := underlyingDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
			return tx.Select(asset.AvailableQty.ColumnName().String(), asset.FrozenQty.ColumnName().String()).
				Updates(&userAsset)
		})
		if _, err := tx.Exec(updateSql); err != nil {
			return err
		}

		return nil
	}); err != nil {
		logx.Errorw("callWithDB  failed", logger.ErrorField(err))
		return nil, err
	}

	return &pb.Empty{}, nil
}
