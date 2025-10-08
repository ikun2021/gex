package logic

import (
	"context"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetErrorCodeListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetErrorCodeListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetErrorCodeListLogic {
	return &GetErrorCodeListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetErrorCodeListLogic) GetErrorCodeList(req *types.GetErrorCodeListReq) (resp *types.GetErrorCodeListResp, err error) {
	errorCode := l.svcCtx.AdminQuery.ErrorCode
	data, count, err := errorCode.WithContext(l.ctx).
		Where(errorCode.Language.Eq(req.Language)).
		Order(errorCode.ID.Desc()).
		FindByPage(0, 1000)
	if err != nil {
		return nil, err
	}
	list := make([]*types.ErrorCode, 0, len(data))
	for _, v := range data {
		c := &types.ErrorCode{
			Id:            uint32(v.ID),
			ErrorCodeName: v.ErrorCodeName,
			ErrorCodeId:   v.ErrorCodeID,
			Language:      v.Language,
		}
		list = append(list, c)
	}

	return &types.GetErrorCodeListResp{
		List:  list,
		Total: count,
	}, nil
}
