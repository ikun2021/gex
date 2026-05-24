package account

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/client/accountservice"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

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
	rpcResp, err := l.svcCtx.AccountRpc.Login(l.ctx, &accountservice.LoginReq{
		Username: req.Username,
		Password: req.Password,
	})
	if err != nil {
		l.Logger.Errorf("login failed: %v", err)
		return nil, err
	}

	return &types.LoginResp{
		Uid:        rpcResp.Uid,
		Username:   rpcResp.Username,
		Token:      rpcResp.Token,
		ExpireTime: rpcResp.ExpireTime,
	}, nil
}
