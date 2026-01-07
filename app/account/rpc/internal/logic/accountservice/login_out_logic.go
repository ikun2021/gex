package accountservicelogic

import (
	"context"
	"github.com/gookit/goutil/strutil"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	logger "github.com/luxun9527/zlog"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginOutLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginOutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginOutLogic {
	return &LoginOutLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 登出
func (l *LoginOutLogic) LoginOut(in *pb.LoginOutReq) (*pb.Empty, error) {
	_, err := l.svcCtx.JwtClient.ParseToken(in.Token)
	if err != nil {
		return nil, errs.TokenValidateFailed
	}
	tokenMd5 := strutil.Md5(in.Token)
	if _, err := l.svcCtx.RedisClient.DelCtx(l.ctx, define.AccountToken.WithParams(tokenMd5)); err != nil {
		logx.Errorw("redis del token failed", logger.ErrorField(err))
		return nil, err
	}

	return &pb.Empty{}, nil
}
