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

type ValidateTokenLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewValidateTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ValidateTokenLogic {
	return &ValidateTokenLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ValidateToken 验证 JWT 签名，并检查 Redis 中是否为当前有效会话（单点登录）。
func (l *ValidateTokenLogic) ValidateToken(in *pb.ValidateTokenReq) (*pb.ValidateTokenResp, error) {
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
		logx.Errorw("parse token failed", logger.ErrorField(err))
		return nil, errs.TokenValidateFailed
	}

	userId := claims.Extra.UserId
	if userId <= 0 {
		return nil, errs.TokenValidateFailed
	}

	ok, err := validateSession(l.ctx, l.svcCtx, userId, in.Token)
	if err != nil {
		logx.Errorw("validate session failed", logger.ErrorField(err))
		return nil, errs.RedisErr
	}
	if !ok {
		return nil, errs.TokenValidateFailed
	}

	username := ""
	user, err := l.svcCtx.UserRepo.FindByID(l.ctx, userId)
	if err != nil {
		logx.Errorw("find user failed", logger.ErrorField(err))
		return nil, errs.Internal
	}
	if user != nil {
		username = user.Username
	}

	return &pb.ValidateTokenResp{
		Uid:      userId,
		Username: username,
	}, nil
}
