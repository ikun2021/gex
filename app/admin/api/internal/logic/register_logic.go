package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/admin/api/internal/dao/admin/model"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/utils"
	"gorm.io/gorm"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type RegisterLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RegisterLogic) Register(req *types.RegisterReq) (resp *types.Empty, err error) {
	if req.ConfirmPassword != req.Password {
		return nil, errs.PasswordNotMatch
	}

	u := l.svcCtx.AdminQuery.User
	p := utils.BcryptHash(req.Password)
	user := &model.User{
		Username: req.Username,
		Password: p,
	}
	if err := u.WithContext(l.ctx).Create(user); err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil, errs.DuplicateDataErr
		}
		logx.Errorw("create user error", logx.Field("err", err))
		return nil, err
	}
	return &types.Empty{}, nil
}
