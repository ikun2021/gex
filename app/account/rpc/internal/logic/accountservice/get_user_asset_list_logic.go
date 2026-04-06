package accountservicelogic

import (
	"context"
	"github.com/ikun2021/gex/common/errs"
	logger "github.com/ikun2021/zlog"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserAssetListLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserAssetListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserAssetListLogic {
	return &GetUserAssetListLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetUserAssetList 获取用户所有币中资产。
func (l *GetUserAssetListLogic) GetUserAssetList(in *pb.GetUserAssetListReq) (*pb.GetUserAssetListResp, error) {
	asset := l.svcCtx.Query.Asset
	result, err := asset.WithContext(l.ctx).
		Where(asset.UserID.Eq(in.Uid)).
		Omit(asset.UpdatedAt, asset.CreatedAt).
		Find()

	if err != nil {
		logx.Errorw("find user asset failed", logger.ErrorField(err))
		return nil, errs.ExecSqlFailed
	}

	assets := make([]*pb.Asset, 0, len(result))

	return &pb.GetUserAssetListResp{AssetList: assets}, nil
}
