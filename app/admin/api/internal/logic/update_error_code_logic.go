package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/admin/api/internal/dao/admin/model"
	"github.com/ikun2021/gex/common/errs"
	"gorm.io/gorm"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateErrorCodeLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateErrorCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateErrorCodeLogic {
	return &UpdateErrorCodeLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateErrorCodeLogic) UpdateErrorCode(req *types.UpdateErrorCodeReq) (resp *types.Empty, err error) {
	code := &model.ErrorCode{
		ID:            int32(req.Id),
		ErrorCodeID:   req.ErrorCodeId,
		ErrorCodeName: req.ErrorCodeName,
		Language:      req.Language,
	}
	errorCode := l.svcCtx.AdminQuery.ErrorCode
	if _, err := errorCode.WithContext(l.ctx).Updates(code); err != nil {
		if errors.Is(gorm.ErrDuplicatedKey, err) {
			return nil, errs.DuplicateDataErr
		}
		return nil, err
	}

	return
}
