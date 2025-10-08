package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/admin/api/internal/dao/admin/model"
	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"
	"github.com/ikun2021/gex/common/errs"
	logger "github.com/luxun9527/zlog"
	"gorm.io/gorm"

	"github.com/zeromicro/go-zero/core/logx"
)

type AddErrorCodeLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAddErrorCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddErrorCodeLogic {
	return &AddErrorCodeLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *AddErrorCodeLogic) AddErrorCode(req *types.AddErrorCodeReq) (resp *types.Empty, err error) {

	errorCode := l.svcCtx.AdminQuery.ErrorCode
	code := &model.ErrorCode{
		ID:            0,
		ErrorCodeID:   req.ErrorCodeId,
		ErrorCodeName: req.ErrorCodeName,
		Language:      req.Language,
		CreatedAt:     0,
		UpdatedAt:     0,
		DeletedAt:     0,
	}

	if err := errorCode.WithContext(l.ctx).Create(code); err != nil {
		if errors.Is(gorm.ErrDuplicatedKey, err) {
			return nil, errs.DuplicateDataErr
		}
		logx.Errorw("create error code failed", logger.ErrorField(err))
		return nil, err
	}

	return &types.Empty{}, nil
}
