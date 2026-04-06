package logic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"
	"gorm.io/gorm"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LoginLogic) Login(req *types.LoginReq) (resp *types.LoginResp, err error) {

	user := l.svcCtx.AdminQuery.User

	u, err := user.WithContext(l.ctx).
		Select(user.Username, user.ID, user.Password).
		Where(user.Username.Eq(req.Username)).
		Take()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.LoginFailed
		}
		logx.Errorw("find user failed", logger.ErrorField(err))
		return nil, err
	}
	if !utils.BcryptCheck(req.Password, u.Password) {
		return nil, errs.LoginFailed
	}
	claims := l.svcCtx.JwtClient.CreateClaims(utils.JwtContent{
		UserID:   int64(u.ID),
		Username: u.Username,
		NickName: "",
	})
	token, err := l.svcCtx.JwtClient.CreateToken(claims)
	if err != nil {
		return nil, err
	}

	return &types.LoginResp{
		Token:  token,
		Expire: 0,
		UserInfo: &types.UserInfo{
			Username: req.Username,
			UserId:   u.ID,
		},
	}, nil
}
