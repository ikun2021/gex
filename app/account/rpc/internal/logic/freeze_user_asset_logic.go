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

type FreezeUserAssetLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFreezeUserAssetLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FreezeUserAssetLogic {
	return &FreezeUserAssetLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 冻结用户资产。
func (l *FreezeUserAssetLogic) FreezeUserAsset(in *pb.FreezeUserAssetReq) (*pb.Empty, error) {
	asset := l.svcCtx.Query.Asset
	barrier, err := dtmgrpc.BarrierFromGrpc(l.ctx)
	if err != nil {
		logx.Errorw("FreezeUserAsset BarrierFromGrpc db failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.DtmErr)
	}

	underlyingDB := asset.WithContext(l.ctx).UnderlyingDB()
	var ma model.Asset
	querySql := underlyingDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
		db := tx.Select(asset.ID.ColumnName().String(), asset.AvailableQty.ColumnName().String(), asset.FrozenQty.ColumnName().String()).
			Clauses(asset.UserID.Eq(in.Uid)).
			Clauses(asset.CoinID.Eq(in.CoinId)).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Take(&ma)
		return db
	})

	db, err := underlyingDB.DB()
	if err != nil {
		logx.Errorw("FreezeUserAsset get db failed", logger.ErrorField(err))
		return nil, errs.CastToDtmError(errs.ExecSqlFailed)
	}

	logx.Infow("req", logx.Field("data", in))
	if err := barrier.CallWithDB(db, func(tx *sql.Tx) error {
		var userAsset model.Asset
		row := tx.QueryRow(querySql)
		if row.Err() != nil {
			return row.Err()
		}

		if err := row.Scan(&userAsset.ID, &userAsset.AvailableQty, &userAsset.FrozenQty); err != nil {
			logx.Errorw("scan data failed", logger.ErrorField(err))
			return errs.CastToDtmError(errs.UserNotFound)
		}
		if userAsset.ID == 0 {
			return errs.CastToDtmError(errs.UserNotFound)
		}

		availableQty := utils.NewFromStringMaxPrec(userAsset.AvailableQty)
		qty := utils.NewFromStringMaxPrec(in.Qty)
		if availableQty.LessThan(qty) {
			return errs.CastToDtmError(errs.AmountInsufficient)
		}
		userAsset.FrozenQty = utils.NewFromStringMaxPrec(userAsset.FrozenQty).Add(qty).String()
		userAsset.AvailableQty = availableQty.Sub(qty).String()

		updateSql := underlyingDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
			return tx.Select(asset.AvailableQty.ColumnName().String(), asset.FrozenQty.ColumnName().String()).
				Updates(&userAsset)
		})
		if _, err := tx.Exec(updateSql); err != nil {
			return errs.CastToDtmError(errs.ExecSqlFailed)
		}

		return nil
	}); err != nil {
		logx.Errorw("callWithDB  failed", logger.ErrorField(err))
		return nil, err
	}

	return &pb.Empty{}, nil
}
