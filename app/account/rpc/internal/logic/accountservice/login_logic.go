package accountservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Login 账号密码登录，JWT 仅含 userId，Redis 保证单点登录。
func (l *LoginLogic) Login(in *pb.LoginReq) (*pb.LoginResp, error) {
	if in.Username == "" || in.Password == "" {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "username and password are required")
	}
	user, err := l.svcCtx.UserRepo.FindByUsername(l.ctx, in.Username)
	if err != nil {
		logx.Errorw("find user failed", logger.ErrorField(err))
		return nil, errs.Internal
	}
	if user == nil || !utils.BcryptCheck(in.Password, user.Password) {
		return nil, errs.LoginFailed
	}
	if user.Status != 1 {
		return nil, errs.LoginFailed
	}

	token, expireAt, err := l.svcCtx.JwtClient.CreateToken(user.ID)
	if err != nil {
		logx.Errorw("create token failed", logger.ErrorField(err))
		return nil, errs.Internal
	}
	if err := saveSession(l.ctx, l.svcCtx, user.ID, token, jwtTTL(l.svcCtx)); err != nil {
		logx.Errorw("save session failed", logger.ErrorField(err))
		return nil, errs.RedisErr
	}

	return &pb.LoginResp{
		Uid:        user.ID,
		Username:   user.Username,
		Token:      token,
		ExpireTime: expireAt,
	}, nil
}
