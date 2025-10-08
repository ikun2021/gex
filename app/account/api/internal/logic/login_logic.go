package logic

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/accountservice"
	"github.com/ikun2021/gex/common/errs"
	logger "github.com/luxun9527/zlog"

	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/app/account/api/internal/types"

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
	// 验证验证码
	if !l.svcCtx.CaptchaStore.Verify(req.CaptchaId, req.Captcha, true) {
		return nil, errs.CaptchaValidateFailed
	}
	loginResp, err := l.svcCtx.AccountRpcClient.Login(l.ctx, &accountservice.LoginReq{
		Username: req.Username,
		Password: req.Password,
	})
	if err != nil {
		logx.Errorw("call account login failed", logger.ErrorField(err))
		return nil, err
	}
	resp = &types.LoginResp{
		Uid:        loginResp.Uid,
		Username:   loginResp.Username,
		Token:      loginResp.Token,
		ExpireTime: loginResp.ExpireTime,
	}
	return
}
