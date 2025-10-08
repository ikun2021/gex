package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type GetUserAssetByCoinLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserAssetByCoinLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserAssetByCoinLogic {
	return &GetUserAssetByCoinLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取用户指定币种的资产
func (l *GetUserAssetByCoinLogic) GetUserAssetByCoin(in *pb.GetUserAssetReq) (*pb.GetUserAssetResp, error) {
	asset := l.svcCtx.Query.Asset
	result, err := asset.WithContext(l.ctx).
		Where(asset.CoinID.Eq(in.CoinId), asset.UserID.Eq(in.Uid)).
		Omit(asset.UpdatedAt, asset.CreatedAt).
		Take()

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.RecordNotFoundErr
		}
		return nil, errs.ExecSqlFailed
	}

	return &pb.GetUserAssetResp{Asset: &pb.Asset{
		Id:           result.ID,
		UserId:       result.ID,
		Username:     result.Username,
		CoinId:       result.CoinID,
		CoinName:     result.CoinName,
		AvailableQty: result.AvailableQty,
		FrozenQty:    result.FrozenQty,
	}}, nil
}
