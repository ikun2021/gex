package accountservicelogic

import (
	"context"
	"github.com/gookit/goutil/strutil"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
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
	if err := l.svcCtx.RedisCli.Del(context.Background(), define.AccountToken.WithParams(tokenMd5)).Err(); err != nil {
		logx.Errorf("redis del token failed %v", err)
		return nil, err
	}

	return &pb.Empty{}, nil
}
