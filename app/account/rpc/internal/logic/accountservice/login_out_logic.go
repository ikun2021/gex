package accountservicelogic

import (
	"context"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/ikun2021/zlog"
	"google.golang.org/grpc/status"

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

// LoginOut 登出，清除 Redis 中的会话。
func (l *LoginOutLogic) LoginOut(in *pb.LoginOutReq) (*pb.Empty, error) {
	if in.Token == "" {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "token is required")
	}

	claims, err := l.svcCtx.JwtClient.ParseToken(in.Token)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if uint32(st.Code()) == utils.TokenExpiredCode {
				return nil, errs.TokenExpire
			}
		}
		return nil, errs.TokenValidateFailed
	}

	if err := revokeSession(l.ctx, l.svcCtx, claims.Extra.UserId, in.Token); err != nil {
		logx.Errorw("revoke session failed", logger.ErrorField(err))
		return nil, errs.RedisErr
	}

	return &pb.Empty{}, nil
}
